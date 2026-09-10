use std::collections::{BTreeMap, BTreeSet};

use crate::{
    CatalogEdge, CatalogError, CatalogObject, CatalogPayload, CatalogResult, ConstraintPayload,
    EdgeKind, ObjectClass, ObjectId, ObjectKind,
};

pub const MAX_DEPENDENCY_DEPTH: usize = 256;

/// Fully resolved, immutable logical catalog graph.
///
/// Construction is deliberately two-pass: all object headers are registered
/// first; only then are parent, owner and edge targets resolved. Physical
/// object or edge order therefore cannot control readiness.
#[derive(Debug, Clone)]
pub struct CatalogGraph {
    objects: BTreeMap<ObjectId, CatalogObject>,
    edges: Vec<CatalogEdge>,
    outgoing: BTreeMap<ObjectId, Vec<usize>>,
    incoming: BTreeMap<ObjectId, Vec<usize>>,
}

impl CatalogGraph {
    pub fn build(objects: Vec<CatalogObject>, mut edges: Vec<CatalogEdge>) -> CatalogResult<Self> {
        let mut registered = BTreeMap::new();
        for object in objects {
            let id = object.id();
            if registered.insert(id, object).is_some() {
                return Err(CatalogError::DuplicateCatalogObject { id: id.to_string() });
            }
        }

        validate_headers_and_names(&registered)?;

        edges.sort_unstable();
        for pair in edges.windows(2) {
            if pair[0] == pair[1] {
                let edge = pair[0];
                return Err(CatalogError::DuplicateCatalogEdge {
                    source: edge.source_object_id().to_string(),
                    target: edge.target_object_id().to_string(),
                    kind: edge.kind().name(),
                    ordinal: edge.ordinal(),
                });
            }
        }

        let mut outgoing: BTreeMap<ObjectId, Vec<usize>> = BTreeMap::new();
        let mut incoming: BTreeMap<ObjectId, Vec<usize>> = BTreeMap::new();
        for (index, edge) in edges.iter().copied().enumerate() {
            validate_edge_endpoints_and_shape(&registered, edge)?;
            outgoing
                .entry(edge.source_object_id())
                .or_default()
                .push(index);
            incoming
                .entry(edge.target_object_id())
                .or_default()
                .push(index);
        }

        let graph = Self {
            objects: registered,
            edges,
            outgoing,
            incoming,
        };
        graph.validate_containment()?;
        graph.validate_ownership()?;
        graph.validate_payload_relations()?;
        graph.validate_acl_keys()?;
        graph.validate_dependency_dag()?;
        graph.validate_role_membership_dag()?;
        Ok(graph)
    }

    pub fn object(&self, id: ObjectId) -> Option<&CatalogObject> {
        self.objects.get(&id)
    }

    pub fn objects(&self) -> impl ExactSizeIterator<Item = &CatalogObject> {
        self.objects.values()
    }

    pub fn edges(&self) -> &[CatalogEdge] {
        &self.edges
    }

    pub fn required_format_minor(&self) -> u16 {
        self.objects
            .values()
            .map(|object| object.kind().minimum_catalog_minor())
            .chain(
                self.edges
                    .iter()
                    .map(|edge| edge.kind().minimum_catalog_minor()),
            )
            .max()
            .unwrap_or(crate::BASELINE_CATALOG_MINOR)
    }

    pub fn outgoing_edges(&self, id: ObjectId) -> impl Iterator<Item = &CatalogEdge> + Clone {
        self.outgoing
            .get(&id)
            .into_iter()
            .flatten()
            .map(|index| &self.edges[*index])
    }

    pub fn incoming_edges(&self, id: ObjectId) -> impl Iterator<Item = &CatalogEdge> + Clone {
        self.incoming
            .get(&id)
            .into_iter()
            .flatten()
            .map(|index| &self.edges[*index])
    }

    pub fn children(&self, id: ObjectId) -> impl Iterator<Item = &CatalogObject> {
        self.outgoing_edges(id)
            .filter(|edge| edge.kind() == EdgeKind::Contains)
            .filter_map(|edge| self.object(edge.target_object_id()))
    }

    pub fn dependents(&self, id: ObjectId) -> impl Iterator<Item = &CatalogObject> {
        self.incoming_edges(id)
            .filter(|edge| edge.kind().is_dependency())
            .filter_map(|edge| self.object(edge.source_object_id()))
    }

    fn validate_containment(&self) -> CatalogResult<()> {
        for object in self.objects.values() {
            let contains = self
                .incoming_edges(object.id())
                .filter(|edge| edge.kind() == EdgeKind::Contains)
                .collect::<Vec<_>>();
            if !object.kind().requires_containment_parent() {
                if !contains.is_empty() {
                    return invalid_object(object.id(), "global object has a Contains parent");
                }
                continue;
            }
            if object.id() == ObjectId::BOOTSTRAP_NAMESPACE {
                if !contains.is_empty() {
                    return invalid_object(object.id(), "bootstrap namespace has a parent edge");
                }
                continue;
            }
            if contains.len() != 1 {
                return invalid_object(
                    object.id(),
                    "every non-bootstrap object requires exactly one Contains parent",
                );
            }
            if Some(contains[0].source_object_id()) != object.parent_id() {
                return invalid_object(object.id(), "Contains edge disagrees with parent_id");
            }

            let mut cursor = object.id();
            let mut depth = 0_usize;
            let mut seen = BTreeSet::new();
            while cursor != ObjectId::BOOTSTRAP_NAMESPACE {
                if !seen.insert(cursor) {
                    return Err(CatalogError::CatalogDependencyCycle {
                        id: cursor.to_string(),
                    });
                }
                depth += 1;
                if depth > MAX_DEPENDENCY_DEPTH {
                    return Err(CatalogError::CatalogDependencyDepthExceeded {
                        id: object.id().to_string(),
                        depth,
                        limit: MAX_DEPENDENCY_DEPTH,
                    });
                }
                cursor = self
                    .object(cursor)
                    .and_then(CatalogObject::parent_id)
                    .ok_or_else(|| CatalogError::InvalidCatalogObject {
                        id: object.id().to_string(),
                        detail: "containment chain does not reach bootstrap namespace",
                    })?;
            }
        }
        Ok(())
    }

    fn validate_payload_relations(&self) -> CatalogResult<()> {
        for object in self.objects.values() {
            match object.payload() {
                CatalogPayload::Namespace(_) => {}
                CatalogPayload::Table(payload) => self.validate_table(object, payload)?,
                CatalogPayload::Column(payload) => self.validate_column(object, payload)?,
                CatalogPayload::Constraint(payload) => {
                    self.validate_constraint(object, payload)?;
                }
                CatalogPayload::Index(payload) => self.validate_index(object, payload)?,
                CatalogPayload::View(payload) => {
                    let actual = self.dependency_targets(object.id())?;
                    if actual != payload.dependency_ids() {
                        return invalid_object(
                            object.id(),
                            "view dependency IDs disagree with dependency edges",
                        );
                    }
                }
                CatalogPayload::Principal(_) | CatalogPayload::Role(_) => {}
                CatalogPayload::AclEntry(payload) => self.validate_acl_entry(object, payload)?,
                CatalogPayload::Function(payload) => match payload {
                    crate::FunctionPayload::Procedural(definition) => {
                        self.validate_routine(object, definition)?;
                    }
                    crate::FunctionPayload::Native(definition) => {
                        self.validate_native_function(object, definition)?;
                    }
                },
                CatalogPayload::Procedure(payload) => {
                    self.validate_routine(object, payload.definition())?;
                }
                CatalogPayload::Trigger(payload) => self.validate_trigger(object, payload)?,
                CatalogPayload::Job(payload) => self.validate_job(object, payload)?,
                CatalogPayload::Extension(payload) => {
                    if payload.package_id() != object.id() {
                        return invalid_object(
                            object.id(),
                            "extension package UUID differs from binding object ID",
                        );
                    }
                }
                CatalogPayload::ExternalType(payload) => {
                    self.validate_external_type(object, payload)?;
                }
                CatalogPayload::Operator(payload) => self.validate_operator(object, payload)?,
                CatalogPayload::OperatorClass(payload) => {
                    self.validate_operator_class(object, payload)?;
                }
                CatalogPayload::PlannerSupport(payload) => {
                    self.validate_planner_support(object, payload)?;
                }
            }
        }
        Ok(())
    }

    fn validate_column(
        &self,
        object: &CatalogObject,
        payload: &crate::ColumnPayload,
    ) -> CatalogResult<()> {
        let dependencies = self.dependency_targets(object.id())?;
        match payload.data_type().type_object_id() {
            Some(type_id) => {
                if dependencies != [type_id] {
                    return invalid_object(
                        object.id(),
                        "external column requires exactly one dependency on its type",
                    );
                }
                let type_object =
                    self.required_kind("external column type", type_id, ObjectKind::ExternalType)?;
                let CatalogPayload::ExternalType(type_payload) = type_object.payload() else {
                    unreachable!()
                };
                if type_payload.write_codec_version() != payload.data_type().parameter_1() {
                    return invalid_object(
                        object.id(),
                        "external column codec version differs from type descriptor",
                    );
                }
            }
            None if dependencies.is_empty() => {}
            None => {
                return invalid_object(
                    object.id(),
                    "built-in column cannot carry type dependency edges",
                );
            }
        }
        Ok(())
    }

    fn validate_external_type(
        &self,
        object: &CatalogObject,
        payload: &crate::ExternalTypePayload,
    ) -> CatalogResult<()> {
        let expected = radixdb_core::derive_plugin_object_identity_bytes(
            payload.extension_binding_id().into_bytes(),
            payload.local_id(),
        )
        .map_err(|_| CatalogError::InvalidCatalogObject {
            id: object.id().to_string(),
            detail: "external type local identity cannot be derived",
        })?;
        if object.id().into_bytes() != expected {
            return invalid_object(
                object.id(),
                "external type object ID differs from package/local-id identity",
            );
        }
        let extension = self.required_kind(
            "external type extension",
            payload.extension_binding_id(),
            ObjectKind::Extension,
        )?;
        if object.owner_principal_id() != extension.owner_principal_id() {
            return invalid_object(
                object.id(),
                "external type owner differs from extension binding owner",
            );
        }
        if self.dependency_targets(object.id())? != [payload.extension_binding_id()] {
            return invalid_object(
                object.id(),
                "external type requires exactly one dependency on its extension binding",
            );
        }
        Ok(())
    }

    fn validate_operator(
        &self,
        object: &CatalogObject,
        payload: &crate::OperatorPayload,
    ) -> CatalogResult<()> {
        self.validate_plugin_local_identity(
            object,
            payload.extension_binding_id(),
            payload.local_id(),
            "operator",
        )?;
        let function = self.required_kind(
            "operator backing function",
            payload.backing_function_id(),
            ObjectKind::Function,
        )?;
        let CatalogPayload::Function(function) = function.payload() else {
            unreachable!()
        };
        if function.native_definition().is_none() {
            return invalid_object(object.id(), "operator backing function is not native");
        }
        let expected_arguments = [payload.left_argument(), payload.right_argument()]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let actual_arguments = function
            .arguments()
            .iter()
            .map(crate::RoutineArgument::data_type)
            .collect::<Vec<_>>();
        let result_matches = matches!(
            function.result(),
            crate::RoutineResult::Scalar { data_type, .. } if *data_type == payload.result_type()
        );
        if expected_arguments != actual_arguments || !result_matches {
            return invalid_object(
                object.id(),
                "operator and backing function signatures differ",
            );
        }
        let mut expected = vec![
            payload.extension_binding_id(),
            payload.backing_function_id(),
        ];
        for data_type in expected_arguments
            .into_iter()
            .chain([payload.result_type()])
        {
            if let Some(id) = data_type.type_object_id() {
                expected.push(id);
            }
        }
        expected.sort_unstable();
        expected.dedup();
        if self.dependency_targets(object.id())? != expected {
            return invalid_object(
                object.id(),
                "operator dependency edges disagree with payload",
            );
        }
        Ok(())
    }

    fn validate_operator_class(
        &self,
        object: &CatalogObject,
        payload: &crate::OperatorClassPayload,
    ) -> CatalogResult<()> {
        self.validate_plugin_local_identity(
            object,
            payload.extension_binding_id(),
            payload.local_id(),
            "operator class",
        )?;
        let input_type_id = payload.input_type().type_object_id().ok_or_else(|| {
            CatalogError::InvalidCatalogObject {
                id: object.id().to_string(),
                detail: "operator class input is not an external type",
            }
        })?;
        let input = self.required_kind(
            "operator class input type",
            input_type_id,
            ObjectKind::ExternalType,
        )?;
        let CatalogPayload::ExternalType(input) = input.payload() else {
            unreachable!()
        };
        if input.extension_binding_id() != payload.extension_binding_id()
            || input.write_codec_version() != payload.input_type().parameter_1()
        {
            return invalid_object(object.id(), "operator class input type binding is stale");
        }
        let mut expected = vec![payload.extension_binding_id(), input_type_id];
        for binding in payload.strategies() {
            self.required_kind(
                "operator class strategy",
                binding.object_id(),
                ObjectKind::Operator,
            )?;
            expected.push(binding.object_id());
        }
        if !payload.supports().is_empty() {
            return invalid_object(
                object.id(),
                "operator-class support slots are reserved; planner support is reverse-bound through PlannerSupport objects",
            );
        }
        expected.sort_unstable();
        expected.dedup();
        if self.dependency_targets(object.id())? != expected {
            return invalid_object(
                object.id(),
                "operator class dependency edges disagree with payload",
            );
        }
        Ok(())
    }

    fn validate_planner_support(
        &self,
        object: &CatalogObject,
        payload: &crate::PlannerSupportPayload,
    ) -> CatalogResult<()> {
        self.validate_plugin_local_identity(
            object,
            payload.extension_binding_id(),
            payload.local_id(),
            "planner support",
        )?;
        let mut expected = vec![payload.extension_binding_id()];
        if let Some(function_id) = payload.target_function_id() {
            let function = self.required_kind(
                "planner support target function",
                function_id,
                ObjectKind::Function,
            )?;
            let CatalogPayload::Function(crate::FunctionPayload::Native(definition)) =
                function.payload()
            else {
                return invalid_object(
                    object.id(),
                    "planner support target must be a native function",
                );
            };
            if definition.extension_binding_id() != payload.extension_binding_id() {
                return invalid_object(
                    object.id(),
                    "planner support and target function use different extensions",
                );
            }
            if definition.volatility() != crate::Volatility::Immutable
                || !matches!(
                    definition.result(),
                    crate::RoutineResult::Scalar { data_type, .. }
                        if data_type.logical_type() == radixdb_core::DataType::Boolean
                )
            {
                return invalid_object(
                    object.id(),
                    "planner support target is not an IMMUTABLE BOOLEAN native function",
                );
            }
            expected.push(function_id);
        }
        if let Some(operator_class_id) = payload.target_operator_class_id() {
            let operator_class = self.required_kind(
                "planner support target operator class",
                operator_class_id,
                ObjectKind::OperatorClass,
            )?;
            let CatalogPayload::OperatorClass(operator_class) = operator_class.payload() else {
                unreachable!()
            };
            if operator_class.extension_binding_id() != payload.extension_binding_id() {
                return invalid_object(
                    object.id(),
                    "planner support and target operator class use different extensions",
                );
            }
            expected.push(operator_class_id);
        }
        expected.sort_unstable();
        expected.dedup();
        if self.dependency_targets(object.id())? != expected {
            return invalid_object(
                object.id(),
                "planner support dependency edges disagree with payload",
            );
        }
        Ok(())
    }

    fn validate_plugin_local_identity(
        &self,
        object: &CatalogObject,
        extension_id: ObjectId,
        local_id: &str,
        role: &'static str,
    ) -> CatalogResult<()> {
        let expected =
            radixdb_core::derive_plugin_object_identity_bytes(extension_id.into_bytes(), local_id)
                .map_err(|_| CatalogError::InvalidCatalogObject {
                    id: object.id().to_string(),
                    detail: "plugin local identity cannot be derived",
                })?;
        if object.id().into_bytes() != expected {
            return invalid_object(
                object.id(),
                "plugin object ID differs from package/local-id identity",
            );
        }
        let extension = self.required_kind(role, extension_id, ObjectKind::Extension)?;
        if object.owner_principal_id() != extension.owner_principal_id() {
            return invalid_object(
                object.id(),
                "plugin object owner differs from extension owner",
            );
        }
        Ok(())
    }

    fn validate_table(
        &self,
        table: &CatalogObject,
        payload: &crate::TablePayload,
    ) -> CatalogResult<()> {
        let mut columns = Vec::new();
        let mut constraints = Vec::new();
        let mut indexes = Vec::new();
        for edge in self
            .outgoing_edges(table.id())
            .filter(|edge| edge.kind() == EdgeKind::Contains)
        {
            let child = self.required_object("table child", edge.target_object_id())?;
            match child.kind() {
                ObjectKind::Column => {
                    let CatalogPayload::Column(column) = child.payload() else {
                        unreachable!("kind/payload equality is an object invariant")
                    };
                    if edge.ordinal() != column.ordinal() {
                        return invalid_object(
                            child.id(),
                            "column ordinal disagrees with Contains edge",
                        );
                    }
                    columns.push((column.ordinal(), child.id()));
                }
                ObjectKind::Constraint => constraints.push(child.id()),
                ObjectKind::Index => indexes.push(child.id()),
                ObjectKind::Trigger => {}
                _ => {
                    return invalid_object(
                        child.id(),
                        "table contains an object kind outside column/constraint/index/trigger",
                    );
                }
            }
        }
        columns.sort_unstable();
        for (expected, (ordinal, _)) in columns.iter().enumerate() {
            if *ordinal != expected as u32 {
                return invalid_object(table.id(), "column ordinals are not contiguous from zero");
            }
        }
        constraints.sort_unstable();
        indexes.sort_unstable();
        let column_ids = columns.iter().map(|(_, id)| *id).collect::<Vec<_>>();
        if column_ids != payload.column_ids()
            || constraints != payload.constraint_ids()
            || indexes != payload.index_ids()
        {
            return invalid_object(table.id(), "table child lists disagree with Contains edges");
        }
        let primary_ids = constraints
            .iter()
            .filter(|id| {
                self.object(**id).is_some_and(|object| {
                    matches!(
                        object.payload(),
                        CatalogPayload::Constraint(ConstraintPayload::PrimaryKey { .. })
                    )
                })
            })
            .copied()
            .collect::<Vec<_>>();
        if primary_ids.as_slice() != payload.primary_key_constraint_id().as_slice() {
            return invalid_object(
                table.id(),
                "primary-key field must identify the table's sole PK constraint",
            );
        }
        Ok(())
    }

    fn validate_ownership(&self) -> CatalogResult<()> {
        let procedural = self.required_format_minor() >= crate::PROCEDURAL_CATALOG_MINOR;
        if !procedural {
            return Ok(());
        }
        let bootstrap_owner = self.required_kind(
            "bootstrap owner principal",
            ObjectId::BOOTSTRAP_OWNER,
            ObjectKind::Principal,
        )?;
        if bootstrap_owner.owner_principal_id() != ObjectId::BOOTSTRAP_OWNER {
            return invalid_object(
                bootstrap_owner.id(),
                "bootstrap principal owner header must retain its own stable ID",
            );
        }
        for object in self.objects.values() {
            let ownership = self
                .outgoing_edges(object.id())
                .filter(|edge| edge.kind() == EdgeKind::OwnedBy)
                .collect::<Vec<_>>();
            if object.id() == ObjectId::BOOTSTRAP_OWNER {
                if !ownership.is_empty() {
                    return invalid_object(
                        object.id(),
                        "bootstrap principal cannot have a self ownership edge",
                    );
                }
                continue;
            }
            if ownership.len() != 1
                || ownership[0].target_object_id() != object.owner_principal_id()
            {
                return invalid_object(
                    object.id(),
                    "object requires exactly one OwnedBy edge matching owner header",
                );
            }
            self.required_kind(
                "object owner",
                object.owner_principal_id(),
                ObjectKind::Principal,
            )?;
        }
        Ok(())
    }

    fn validate_acl_entry(
        &self,
        object: &CatalogObject,
        payload: &crate::AclEntryPayload,
    ) -> CatalogResult<()> {
        if object.owner_principal_id() != payload.grantor_principal_id() {
            return invalid_object(object.id(), "ACL owner differs from grantor");
        }
        self.required_kind(
            "ACL grantor",
            payload.grantor_principal_id(),
            ObjectKind::Principal,
        )?;
        let grantees = self
            .outgoing_edges(object.id())
            .filter(|edge| edge.kind() == EdgeKind::GrantedTo)
            .collect::<Vec<_>>();
        let targets = self
            .outgoing_edges(object.id())
            .filter(|edge| edge.kind() == EdgeKind::GrantsOn)
            .collect::<Vec<_>>();
        if grantees.len() != 1 || targets.len() != 1 {
            return invalid_object(
                object.id(),
                "ACL entry requires exactly one GrantedTo and one GrantsOn edge",
            );
        }
        let grantee = self.required_object("ACL grantee", grantees[0].target_object_id())?;
        if !matches!(grantee.kind(), ObjectKind::Principal | ObjectKind::Role) {
            return invalid_object(object.id(), "ACL grantee is not Principal or Role");
        }
        let target = self.required_object("ACL target", targets[0].target_object_id())?;
        match payload {
            crate::AclEntryPayload::RoleMembership { .. } if target.kind() != ObjectKind::Role => {
                invalid_object(object.id(), "role membership target is not Role")
            }
            crate::AclEntryPayload::ObjectPrivileges { columns, .. } => {
                for group in columns {
                    for id in group.column_ids() {
                        let column = self.required_kind("ACL column", *id, ObjectKind::Column)?;
                        if target.kind() != ObjectKind::Table
                            || column.parent_id() != Some(target.id())
                        {
                            return invalid_object(
                                object.id(),
                                "column privilege does not belong to target table",
                            );
                        }
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn validate_routine(
        &self,
        object: &CatalogObject,
        definition: &crate::RoutineDefinition,
    ) -> CatalogResult<()> {
        for namespace in definition.search_path() {
            self.required_kind("routine search path", *namespace, ObjectKind::Namespace)?;
        }
        let mut expected = definition.search_path().to_vec();
        expected.extend_from_slice(definition.dependency_ids());
        expected.sort_unstable();
        expected.dedup();
        if self.dependency_targets(object.id())? != expected {
            return invalid_object(
                object.id(),
                "routine referenced IDs disagree with dependency edges",
            );
        }
        Ok(())
    }

    fn validate_native_function(
        &self,
        object: &CatalogObject,
        definition: &crate::NativeFunctionDefinition,
    ) -> CatalogResult<()> {
        let expected_id = radixdb_core::derive_plugin_object_identity_bytes(
            definition.extension_binding_id().into_bytes(),
            definition.local_id(),
        )
        .map_err(|_| CatalogError::InvalidCatalogObject {
            id: object.id().to_string(),
            detail: "native function local identity cannot be derived",
        })?;
        if object.id().into_bytes() != expected_id {
            return invalid_object(
                object.id(),
                "native function object ID differs from package/local-id identity",
            );
        }
        let extension = self.required_kind(
            "native function extension",
            definition.extension_binding_id(),
            ObjectKind::Extension,
        )?;
        if object.owner_principal_id() != extension.owner_principal_id() {
            return invalid_object(
                object.id(),
                "native function owner differs from extension binding owner",
            );
        }
        for argument in definition.arguments() {
            self.validate_native_type_ref(object.id(), argument.data_type())?;
        }
        if let crate::RoutineResult::Scalar { data_type, .. } = definition.result() {
            self.validate_native_type_ref(object.id(), *data_type)?;
        }
        if self.dependency_targets(object.id())? != definition.dependency_ids() {
            return invalid_object(
                object.id(),
                "native function referenced IDs disagree with dependency edges",
            );
        }
        Ok(())
    }

    fn validate_native_type_ref(
        &self,
        owner: ObjectId,
        data_type: crate::CatalogDataType,
    ) -> CatalogResult<()> {
        let Some(type_ref) = data_type.external_type_ref() else {
            return Ok(());
        };
        let id = ObjectId::from_bytes(type_ref.type_object_id())?;
        let target = self.required_kind("native function type", id, ObjectKind::ExternalType)?;
        let CatalogPayload::ExternalType(payload) = target.payload() else {
            unreachable!("kind/payload equality is an object invariant")
        };
        if payload.write_codec_version() != type_ref.codec_version() {
            return invalid_object(owner, "native function external codec version is stale");
        }
        Ok(())
    }

    fn validate_trigger(
        &self,
        object: &CatalogObject,
        payload: &crate::TriggerPayload,
    ) -> CatalogResult<()> {
        if object.parent_id() != Some(payload.table_id()) {
            return invalid_object(object.id(), "trigger table differs from parent");
        }
        self.required_kind("trigger table", payload.table_id(), ObjectKind::Table)?;
        let function = self.required_kind(
            "trigger function",
            payload.function_id(),
            ObjectKind::Function,
        )?;
        let CatalogPayload::Function(function_payload) = function.payload() else {
            unreachable!()
        };
        let Some(function_definition) = function_payload.procedural_definition() else {
            return invalid_object(object.id(), "trigger target is a native function");
        };
        if function_definition.volatility() != crate::Volatility::Volatile
            || !matches!(function_definition.result(), crate::RoutineResult::Trigger)
        {
            return invalid_object(
                object.id(),
                "trigger target is not a VOLATILE RETURNS TRIGGER function",
            );
        }
        self.validate_columns_belong_to(payload.update_column_ids(), payload.table_id())?;
        let mut expected = vec![payload.table_id(), payload.function_id()];
        expected.extend_from_slice(payload.update_column_ids());
        expected.sort_unstable();
        expected.dedup();
        if self.dependency_targets(object.id())? != expected {
            return invalid_object(
                object.id(),
                "trigger referenced IDs disagree with dependency edges",
            );
        }
        Ok(())
    }

    fn validate_job(
        &self,
        object: &CatalogObject,
        payload: &crate::JobPayload,
    ) -> CatalogResult<()> {
        self.required_kind(
            "job procedure",
            payload.procedure_id(),
            ObjectKind::Procedure,
        )?;
        self.required_kind(
            "job principal",
            payload.principal_id(),
            ObjectKind::Principal,
        )?;
        let mut expected = vec![payload.procedure_id(), payload.principal_id()];
        expected.sort_unstable();
        if self.dependency_targets(object.id())? != expected {
            return invalid_object(
                object.id(),
                "job referenced IDs disagree with dependency edges",
            );
        }
        Ok(())
    }

    fn validate_constraint(
        &self,
        object: &CatalogObject,
        payload: &ConstraintPayload,
    ) -> CatalogResult<()> {
        let table_id = object.parent_id().expect("validated non-bootstrap parent");
        let references = self.dependency_targets(object.id())?;
        match payload {
            ConstraintPayload::PrimaryKey { local_column_ids } => {
                self.validate_columns_belong_to(local_column_ids, table_id)?;
                if !references.is_empty() {
                    return invalid_object(object.id(), "non-FK constraint has reference edges");
                }
                for column_id in local_column_ids {
                    if self.required_column_payload(*column_id)?.nullable() {
                        return invalid_object(
                            object.id(),
                            "primary-key column cannot be nullable",
                        );
                    }
                }
            }
            ConstraintPayload::Unique { local_column_ids } => {
                self.validate_columns_belong_to(local_column_ids, table_id)?;
                if !references.is_empty() {
                    return invalid_object(object.id(), "non-FK constraint has reference edges");
                }
            }
            ConstraintPayload::ForeignKey {
                local_column_ids,
                referenced_table_id,
                referenced_column_ids,
                on_update_action,
                on_delete_action,
                ..
            } => {
                self.validate_columns_belong_to(local_column_ids, table_id)?;
                let referenced_table = self.required_kind(
                    "referenced table",
                    *referenced_table_id,
                    ObjectKind::Table,
                )?;
                self.validate_columns_belong_to(referenced_column_ids, referenced_table.id())?;
                let mut expected = Vec::with_capacity(referenced_column_ids.len() + 1);
                expected.push(*referenced_table_id);
                expected.extend(referenced_column_ids.iter().copied());
                expected.sort_unstable();
                if references != expected {
                    return invalid_object(
                        object.id(),
                        "foreign-key payload disagrees with References edges",
                    );
                }
                if !self.table_has_unique_key(*referenced_table_id, referenced_column_ids)? {
                    return invalid_object(
                        object.id(),
                        "foreign key target is neither PRIMARY KEY nor UNIQUE",
                    );
                }
                for (local, referenced) in local_column_ids.iter().zip(referenced_column_ids) {
                    let local_payload = self.required_column_payload(*local)?;
                    let referenced_payload = self.required_column_payload(*referenced)?;
                    if local_payload.data_type() != referenced_payload.data_type() {
                        return invalid_object(
                            object.id(),
                            "foreign-key local/referenced column types differ",
                        );
                    }
                    if (*on_update_action == crate::ForeignKeyAction::SetNull
                        || *on_delete_action == crate::ForeignKeyAction::SetNull)
                        && !local_payload.nullable()
                    {
                        return invalid_object(
                            object.id(),
                            "SET NULL foreign key column is not nullable",
                        );
                    }
                    if (*on_update_action == crate::ForeignKeyAction::SetDefault
                        || *on_delete_action == crate::ForeignKeyAction::SetDefault)
                        && local_payload.default_sql().is_none()
                    {
                        return invalid_object(
                            object.id(),
                            "SET DEFAULT foreign key column has no default",
                        );
                    }
                }
            }
            ConstraintPayload::Check {
                local_column_id, ..
            } => {
                if let Some(local_column_id) = local_column_id {
                    self.validate_columns_belong_to(&[*local_column_id], table_id)?;
                }
                if !references.is_empty() {
                    return invalid_object(object.id(), "CHECK constraint has reference edges");
                }
            }
            ConstraintPayload::NotNull { local_column_id } => {
                self.validate_columns_belong_to(&[*local_column_id], table_id)?;
                if !references.is_empty() {
                    return invalid_object(object.id(), "NOT NULL constraint has reference edges");
                }
                if self.required_column_payload(*local_column_id)?.nullable() {
                    return invalid_object(
                        object.id(),
                        "NOT NULL constraint targets a nullable column",
                    );
                }
            }
        }
        Ok(())
    }

    fn validate_index(
        &self,
        object: &CatalogObject,
        payload: &crate::IndexPayload,
    ) -> CatalogResult<()> {
        let table_id = object.parent_id().expect("validated non-bootstrap parent");
        self.validate_columns_belong_to(payload.key_column_ids(), table_id)?;
        self.validate_columns_belong_to(payload.include_column_ids(), table_id)?;
        if payload.access_method() == crate::AccessMethod::Hnsw {
            let column = self.required_column_payload(payload.key_column_ids()[0])?;
            if column.data_type().logical_type() != radixdb_core::DataType::Vector {
                return invalid_object(object.id(), "HNSW key is not a VECTOR column");
            }
        }
        let dependencies = self.dependency_targets(object.id())?;
        if let Some(operator_class_id) = payload.operator_class_id() {
            if payload.key_column_ids().len() != 1 {
                return invalid_object(object.id(), "operator-class index is not single-column");
            }
            let column = self.required_column_payload(payload.key_column_ids()[0])?;
            let Some(type_id) = column.data_type().type_object_id() else {
                return invalid_object(object.id(), "operator-class index key is not external");
            };
            let operator_class = self.required_kind(
                "index operator class",
                operator_class_id,
                ObjectKind::OperatorClass,
            )?;
            let CatalogPayload::OperatorClass(operator_class) = operator_class.payload() else {
                unreachable!()
            };
            if operator_class.access_method() != payload.access_method()
                || operator_class.input_type() != column.data_type()
            {
                return invalid_object(
                    object.id(),
                    "index/operator-class access method or input type differs",
                );
            }
            let mut expected = vec![operator_class_id, type_id];
            expected.sort_unstable();
            if dependencies != expected {
                return invalid_object(
                    object.id(),
                    "external index requires type and operator-class dependencies",
                );
            }
            return Ok(());
        }
        if dependencies.len() > 1 {
            return invalid_object(object.id(), "index has more than one constraint dependency");
        }
        if let Some(constraint_id) = dependencies.first() {
            let constraint =
                self.required_kind("index constraint", *constraint_id, ObjectKind::Constraint)?;
            if constraint.parent_id() != Some(table_id) {
                return invalid_object(object.id(), "index constraint belongs to another table");
            }
            let expected_columns = match constraint.payload() {
                CatalogPayload::Constraint(ConstraintPayload::PrimaryKey { local_column_ids })
                | CatalogPayload::Constraint(ConstraintPayload::Unique { local_column_ids }) => {
                    local_column_ids
                }
                _ => {
                    return invalid_object(object.id(), "index depends on a non-key constraint");
                }
            };
            if !payload.unique()
                || payload.key_column_ids() != expected_columns
                || payload.expression_sql().is_some()
                || payload.predicate_sql().is_some()
            {
                return invalid_object(
                    object.id(),
                    "constraint-owned index does not match its key constraint",
                );
            }
        }
        Ok(())
    }

    fn validate_columns_belong_to(
        &self,
        column_ids: &[ObjectId],
        table_id: ObjectId,
    ) -> CatalogResult<()> {
        for id in column_ids {
            let column = self.required_kind("column", *id, ObjectKind::Column)?;
            if column.parent_id() != Some(table_id) {
                return invalid_object(*id, "column belongs to a different table");
            }
        }
        Ok(())
    }

    fn required_column_payload(&self, id: ObjectId) -> CatalogResult<&crate::ColumnPayload> {
        let column = self.required_kind("column", id, ObjectKind::Column)?;
        let CatalogPayload::Column(payload) = column.payload() else {
            unreachable!("kind/payload equality is an object invariant")
        };
        Ok(payload)
    }

    fn table_has_unique_key(
        &self,
        table_id: ObjectId,
        expected_columns: &[ObjectId],
    ) -> CatalogResult<bool> {
        let table = self.required_kind("referenced table", table_id, ObjectKind::Table)?;
        let CatalogPayload::Table(payload) = table.payload() else {
            unreachable!("kind/payload equality is an object invariant")
        };
        let constraint_owns_key = payload.constraint_ids().iter().any(|constraint_id| {
            self.object(*constraint_id)
                .is_some_and(|constraint| match constraint.payload() {
                    CatalogPayload::Constraint(ConstraintPayload::PrimaryKey {
                        local_column_ids,
                    })
                    | CatalogPayload::Constraint(ConstraintPayload::Unique {
                        local_column_ids,
                    }) => local_column_ids == expected_columns,
                    _ => false,
                })
        });
        let standalone_index_owns_key = payload.index_ids().iter().any(|index_id| {
            self.object(*index_id)
                .is_some_and(|index| match index.payload() {
                    CatalogPayload::Index(payload) => {
                        payload.unique()
                            && payload.predicate_sql().is_none()
                            && payload.expression_sql().is_none()
                            && payload.key_column_ids() == expected_columns
                    }
                    _ => false,
                })
        });
        Ok(constraint_owns_key || standalone_index_owns_key)
    }

    fn dependency_targets(&self, source: ObjectId) -> CatalogResult<Vec<ObjectId>> {
        let mut targets = self
            .outgoing_edges(source)
            .filter(|edge| edge.kind().is_dependency())
            .map(|edge| edge.target_object_id())
            .collect::<Vec<_>>();
        targets.sort_unstable();
        if let Some(pair) = targets.windows(2).find(|pair| pair[0] == pair[1]) {
            return Err(CatalogError::IllegalCatalogEdge {
                source: source.to_string(),
                target: pair[0].to_string(),
                kind: "dependency",
                detail: "same logical dependency is emitted more than once",
            });
        }
        Ok(targets)
    }

    fn required_object(&self, role: &'static str, id: ObjectId) -> CatalogResult<&CatalogObject> {
        self.object(id)
            .ok_or_else(|| CatalogError::MissingCatalogObject {
                role,
                id: id.to_string(),
            })
    }

    fn required_kind(
        &self,
        role: &'static str,
        id: ObjectId,
        kind: ObjectKind,
    ) -> CatalogResult<&CatalogObject> {
        let object = self.required_object(role, id)?;
        if object.kind() != kind {
            return invalid_object(object.id(), "referenced object has the wrong kind");
        }
        Ok(object)
    }

    fn validate_dependency_dag(&self) -> CatalogResult<()> {
        let mut indegree = self
            .objects
            .keys()
            .copied()
            .map(|id| (id, 0_usize))
            .collect::<BTreeMap<_, _>>();
        for edge in self.edges.iter().filter(|edge| edge.kind().is_dependency()) {
            *indegree
                .get_mut(&edge.target_object_id())
                .expect("edge endpoints were resolved") += 1;
        }
        let mut ready = indegree
            .iter()
            .filter_map(|(id, degree)| (*degree == 0).then_some(*id))
            .collect::<BTreeSet<_>>();
        let mut depth = BTreeMap::<ObjectId, usize>::new();
        let mut visited = 0_usize;
        while let Some(id) = ready.pop_first() {
            visited += 1;
            let source_depth = depth.get(&id).copied().unwrap_or(0);
            for edge in self
                .outgoing_edges(id)
                .filter(|edge| edge.kind().is_dependency())
            {
                let target = edge.target_object_id();
                let target_depth = source_depth + 1;
                if target_depth > MAX_DEPENDENCY_DEPTH {
                    return Err(CatalogError::CatalogDependencyDepthExceeded {
                        id: target.to_string(),
                        depth: target_depth,
                        limit: MAX_DEPENDENCY_DEPTH,
                    });
                }
                depth
                    .entry(target)
                    .and_modify(|known| *known = (*known).max(target_depth))
                    .or_insert(target_depth);
                let degree = indegree
                    .get_mut(&target)
                    .expect("edge endpoints were resolved");
                *degree -= 1;
                if *degree == 0 {
                    ready.insert(target);
                }
            }
        }
        if visited != self.objects.len() {
            let id = indegree
                .into_iter()
                .find_map(|(id, degree)| (degree != 0).then_some(id))
                .expect("unvisited graph has a non-zero indegree");
            return Err(CatalogError::CatalogDependencyCycle { id: id.to_string() });
        }
        Ok(())
    }

    fn validate_role_membership_dag(&self) -> CatalogResult<()> {
        const MAX_ROLE_DEPTH: usize = 64;
        const MAX_ROLE_VISITS: usize = 65_536;
        let subjects = self
            .objects
            .values()
            .filter(|object| matches!(object.kind(), ObjectKind::Principal | ObjectKind::Role))
            .map(CatalogObject::id)
            .collect::<BTreeSet<_>>();
        if subjects.len() > MAX_ROLE_VISITS {
            return Err(CatalogError::CatalogLimitExceeded {
                field: "authorization subjects",
                actual: subjects.len() as u64,
                limit: MAX_ROLE_VISITS as u64,
            });
        }
        let mut outgoing = BTreeMap::<ObjectId, Vec<ObjectId>>::new();
        let mut indegree = subjects
            .iter()
            .copied()
            .map(|id| (id, 0_usize))
            .collect::<BTreeMap<_, _>>();
        for object in self.objects.values() {
            if !matches!(
                object.payload(),
                CatalogPayload::AclEntry(crate::AclEntryPayload::RoleMembership { .. })
            ) {
                continue;
            }
            let member = self
                .outgoing_edges(object.id())
                .find(|edge| edge.kind() == EdgeKind::GrantedTo)
                .expect("ACL shape validated")
                .target_object_id();
            let role = self
                .outgoing_edges(object.id())
                .find(|edge| edge.kind() == EdgeKind::GrantsOn)
                .expect("ACL shape validated")
                .target_object_id();
            outgoing.entry(member).or_default().push(role);
            *indegree
                .get_mut(&role)
                .expect("membership role endpoint validated") += 1;
        }
        let mut ready = indegree
            .iter()
            .filter_map(|(id, degree)| (*degree == 0).then_some(*id))
            .collect::<BTreeSet<_>>();
        let mut depth = BTreeMap::<ObjectId, usize>::new();
        let mut visited = 0;
        while let Some(id) = ready.pop_first() {
            visited += 1;
            let source_depth = depth.get(&id).copied().unwrap_or(0);
            for target in outgoing.get(&id).into_iter().flatten() {
                let target_depth = source_depth + 1;
                if target_depth > MAX_ROLE_DEPTH {
                    return Err(CatalogError::CatalogDependencyDepthExceeded {
                        id: target.to_string(),
                        depth: target_depth,
                        limit: MAX_ROLE_DEPTH,
                    });
                }
                depth
                    .entry(*target)
                    .and_modify(|known| *known = (*known).max(target_depth))
                    .or_insert(target_depth);
                let degree = indegree
                    .get_mut(target)
                    .expect("membership endpoint validated");
                *degree -= 1;
                if *degree == 0 {
                    ready.insert(*target);
                }
            }
        }
        if visited != subjects.len() {
            let id = indegree
                .into_iter()
                .find_map(|(id, degree)| (degree != 0).then_some(id))
                .expect("membership cycle has node");
            return Err(CatalogError::CatalogDependencyCycle { id: id.to_string() });
        }
        Ok(())
    }

    fn validate_acl_keys(&self) -> CatalogResult<()> {
        let mut keys = BTreeSet::new();
        for object in self.objects.values() {
            let CatalogPayload::AclEntry(payload) = object.payload() else {
                continue;
            };
            let grantee = self
                .outgoing_edges(object.id())
                .find(|edge| edge.kind() == EdgeKind::GrantedTo)
                .expect("ACL shape validated")
                .target_object_id();
            let target = self
                .outgoing_edges(object.id())
                .find(|edge| edge.kind() == EdgeKind::GrantsOn)
                .expect("ACL shape validated")
                .target_object_id();
            let discriminator = u8::from(matches!(
                payload,
                crate::AclEntryPayload::RoleMembership { .. }
            ));
            let key = (
                discriminator,
                payload.grantor_principal_id(),
                grantee,
                target,
            );
            if !keys.insert(key) {
                return invalid_object(object.id(), "duplicate ACL grant identity");
            }
        }
        Ok(())
    }
}

fn validate_headers_and_names(objects: &BTreeMap<ObjectId, CatalogObject>) -> CatalogResult<()> {
    let bootstrap = objects.get(&ObjectId::BOOTSTRAP_NAMESPACE).ok_or_else(|| {
        CatalogError::MissingCatalogObject {
            role: "required bootstrap namespace",
            id: ObjectId::BOOTSTRAP_NAMESPACE.to_string(),
        }
    })?;
    if bootstrap.kind() != ObjectKind::Namespace
        || bootstrap.namespace_id().is_some()
        || bootstrap.parent_id().is_some()
    {
        return invalid_object(
            bootstrap.id(),
            "bootstrap ID must be the root Namespace with no namespace/parent",
        );
    }

    let procedural = objects
        .values()
        .any(|object| object.kind().minimum_catalog_minor() >= crate::PROCEDURAL_CATALOG_MINOR);
    if procedural {
        let principal = objects.get(&ObjectId::BOOTSTRAP_OWNER).ok_or_else(|| {
            CatalogError::MissingCatalogObject {
                role: "promoted bootstrap principal",
                id: ObjectId::BOOTSTRAP_OWNER.to_string(),
            }
        })?;
        if principal.kind() != ObjectKind::Principal
            || principal.namespace_id().is_some()
            || principal.parent_id().is_some()
        {
            return invalid_object(
                principal.id(),
                "bootstrap owner promotion is not a global Principal",
            );
        }
    }

    let mut names = BTreeSet::<(Option<ObjectId>, ObjectClass, String)>::new();
    for object in objects.values() {
        if !procedural && object.id() == ObjectId::BOOTSTRAP_OWNER {
            return invalid_object(
                object.id(),
                "bootstrap owner is a sentinel, not a V6.0 catalog object",
            );
        }
        if !procedural && object.owner_principal_id() != ObjectId::BOOTSTRAP_OWNER {
            return invalid_object(
                object.id(),
                "V6.0 owner must be the bootstrap owner sentinel",
            );
        }
        if !object.kind().requires_containment_parent() {
            if object.namespace_id().is_some() || object.parent_id().is_some() {
                return invalid_object(
                    object.id(),
                    "global object cannot have namespace or parent",
                );
            }
        } else if object.id() != ObjectId::BOOTSTRAP_NAMESPACE {
            let namespace_id =
                object
                    .namespace_id()
                    .ok_or_else(|| CatalogError::InvalidCatalogObject {
                        id: object.id().to_string(),
                        detail: "non-bootstrap object requires namespace_id",
                    })?;
            let parent_id =
                object
                    .parent_id()
                    .ok_or_else(|| CatalogError::InvalidCatalogObject {
                        id: object.id().to_string(),
                        detail: "non-bootstrap object requires parent_id",
                    })?;
            if namespace_id == object.id() || parent_id == object.id() {
                return invalid_object(object.id(), "object cannot own itself");
            }
            let namespace =
                objects
                    .get(&namespace_id)
                    .ok_or_else(|| CatalogError::MissingCatalogObject {
                        role: "namespace",
                        id: namespace_id.to_string(),
                    })?;
            if namespace.kind() != ObjectKind::Namespace {
                return invalid_object(object.id(), "namespace_id does not target Namespace");
            }
            let parent =
                objects
                    .get(&parent_id)
                    .ok_or_else(|| CatalogError::MissingCatalogObject {
                        role: "parent",
                        id: parent_id.to_string(),
                    })?;
            validate_parent_namespace_relation(object, parent, namespace_id)?;
        }

        if object.kind() == ObjectKind::AclEntry {
            let expected = format!("acl_{}", object.id());
            if object.name().normalized().as_str() != expected {
                return invalid_object(
                    object.id(),
                    "ACL entry name is not derived from its object ID",
                );
            }
            continue;
        }
        let scope = match object.kind() {
            ObjectKind::Column | ObjectKind::Constraint => object.parent_id(),
            ObjectKind::Trigger => object.parent_id(),
            ObjectKind::Principal | ObjectKind::Role | ObjectKind::Extension => None,
            ObjectKind::Namespace
            | ObjectKind::Table
            | ObjectKind::Index
            | ObjectKind::View
            | ObjectKind::Function
            | ObjectKind::Procedure
            | ObjectKind::Job
            | ObjectKind::ExternalType
            | ObjectKind::Operator
            | ObjectKind::OperatorClass
            | ObjectKind::PlannerSupport => object.namespace_id(),
            ObjectKind::AclEntry => unreachable!(),
        };
        let identity_name = match object.payload() {
            CatalogPayload::Function(payload) => {
                routine_identity_name_from_arguments(object, payload.arguments())
            }
            CatalogPayload::Procedure(payload) => {
                routine_identity_name(object, payload.definition())
            }
            CatalogPayload::Operator(payload) => operator_identity_name(object, payload),
            _ => object.name().normalized().as_str().to_owned(),
        };
        let key = (scope, object.kind().object_class(), identity_name);
        if !names.insert(key.clone()) {
            return Err(CatalogError::DuplicateCatalogName {
                name: format!("{:?}/{:?}/{}", key.0, key.1, key.2),
            });
        }
    }
    Ok(())
}

fn validate_parent_namespace_relation(
    object: &CatalogObject,
    parent: &CatalogObject,
    namespace_id: ObjectId,
) -> CatalogResult<()> {
    match object.kind() {
        ObjectKind::Namespace
        | ObjectKind::Table
        | ObjectKind::View
        | ObjectKind::Function
        | ObjectKind::Procedure
        | ObjectKind::Job
        | ObjectKind::ExternalType
        | ObjectKind::Operator
        | ObjectKind::OperatorClass
        | ObjectKind::PlannerSupport => {
            if parent.kind() != ObjectKind::Namespace || parent.id() != namespace_id {
                return invalid_object(
                    object.id(),
                    "namespace/relation parent must equal namespace_id and be Namespace",
                );
            }
        }
        ObjectKind::Column | ObjectKind::Constraint | ObjectKind::Index | ObjectKind::Trigger => {
            if parent.kind() != ObjectKind::Table || parent.namespace_id() != Some(namespace_id) {
                return invalid_object(
                    object.id(),
                    "table child parent/namespace relationship is inconsistent",
                );
            }
        }
        ObjectKind::Principal | ObjectKind::Role | ObjectKind::AclEntry | ObjectKind::Extension => {
            return invalid_object(object.id(), "global object reached containment validation");
        }
    }
    Ok(())
}

fn operator_identity_name(object: &CatalogObject, payload: &crate::OperatorPayload) -> String {
    let mut output = object.name().normalized().as_str().to_owned();
    output.push('(');
    for data_type in [payload.left_argument(), payload.right_argument()] {
        match data_type {
            Some(value) if value.is_external() => {
                use std::fmt::Write;
                write!(
                    output,
                    "x{}v{}",
                    value.type_object_id().expect("external type has object ID"),
                    value.parameter_1()
                )
                .expect("writing to String cannot fail");
            }
            Some(value) => {
                use std::fmt::Write;
                write!(output, "b{}", value.logical_type() as u8)
                    .expect("writing to String cannot fail");
            }
            None => output.push('_'),
        }
        output.push(',');
    }
    output.push(')');
    output
}

fn validate_edge_endpoints_and_shape(
    objects: &BTreeMap<ObjectId, CatalogObject>,
    edge: CatalogEdge,
) -> CatalogResult<()> {
    let source = objects.get(&edge.source_object_id()).ok_or_else(|| {
        CatalogError::MissingCatalogObject {
            role: "edge source",
            id: edge.source_object_id().to_string(),
        }
    })?;
    let target = objects.get(&edge.target_object_id()).ok_or_else(|| {
        CatalogError::MissingCatalogObject {
            role: "edge target",
            id: edge.target_object_id().to_string(),
        }
    })?;
    if source.id() == target.id() {
        return illegal_edge(edge, "self-edge is invalid");
    }
    let legal = match edge.kind() {
        EdgeKind::Contains => match source.kind() {
            ObjectKind::Namespace => matches!(
                target.kind(),
                ObjectKind::Namespace
                    | ObjectKind::Table
                    | ObjectKind::View
                    | ObjectKind::Function
                    | ObjectKind::Procedure
                    | ObjectKind::Job
                    | ObjectKind::ExternalType
                    | ObjectKind::Operator
                    | ObjectKind::OperatorClass
                    | ObjectKind::PlannerSupport
            ),
            ObjectKind::Table => matches!(
                target.kind(),
                ObjectKind::Column
                    | ObjectKind::Constraint
                    | ObjectKind::Index
                    | ObjectKind::Trigger
            ),
            _ => false,
        },
        EdgeKind::DependsOn => matches!(
            (source.kind(), target.kind()),
            (ObjectKind::View, ObjectKind::Table | ObjectKind::View)
                | (
                    ObjectKind::Index,
                    ObjectKind::Constraint | ObjectKind::ExternalType | ObjectKind::OperatorClass
                )
                | (ObjectKind::Column, ObjectKind::ExternalType)
                | (ObjectKind::ExternalType, ObjectKind::Extension)
                | (
                    ObjectKind::Operator,
                    ObjectKind::Extension | ObjectKind::Function | ObjectKind::ExternalType
                )
                | (
                    ObjectKind::OperatorClass,
                    ObjectKind::Extension | ObjectKind::ExternalType | ObjectKind::Operator
                )
                | (
                    ObjectKind::PlannerSupport,
                    ObjectKind::Extension | ObjectKind::Function | ObjectKind::OperatorClass
                )
                | (
                    ObjectKind::Function
                        | ObjectKind::Procedure
                        | ObjectKind::Trigger
                        | ObjectKind::Job,
                    ObjectKind::Extension
                )
                | (
                    ObjectKind::Function
                        | ObjectKind::Procedure
                        | ObjectKind::Trigger
                        | ObjectKind::Job,
                    ObjectKind::Function
                        | ObjectKind::Procedure
                        | ObjectKind::Trigger
                        | ObjectKind::Job
                )
        ),
        EdgeKind::References => matches!(
            (source.kind(), target.kind()),
            (ObjectKind::View, ObjectKind::Table | ObjectKind::View)
                | (
                    ObjectKind::Operator,
                    ObjectKind::Function | ObjectKind::ExternalType
                )
                | (
                    ObjectKind::OperatorClass,
                    ObjectKind::ExternalType | ObjectKind::Operator
                )
                | (
                    ObjectKind::PlannerSupport,
                    ObjectKind::Function | ObjectKind::OperatorClass
                )
                | (
                    ObjectKind::Constraint,
                    ObjectKind::Table | ObjectKind::Column
                )
                | (
                    ObjectKind::Function
                        | ObjectKind::Procedure
                        | ObjectKind::Trigger
                        | ObjectKind::Job,
                    ObjectKind::Namespace
                        | ObjectKind::Table
                        | ObjectKind::View
                        | ObjectKind::Column
                        | ObjectKind::Function
                        | ObjectKind::Procedure
                        | ObjectKind::ExternalType
                        | ObjectKind::Principal
                )
        ),
        EdgeKind::OwnedBy => target.kind() == ObjectKind::Principal,
        EdgeKind::GrantedTo => {
            source.kind() == ObjectKind::AclEntry
                && matches!(target.kind(), ObjectKind::Principal | ObjectKind::Role)
        }
        EdgeKind::GrantsOn => {
            source.kind() == ObjectKind::AclEntry && target.kind() != ObjectKind::AclEntry
        }
    };
    if !legal {
        return illegal_edge(edge, "source/target object kinds are not admitted");
    }
    Ok(())
}

fn routine_identity_name(object: &CatalogObject, definition: &crate::RoutineDefinition) -> String {
    routine_identity_name_from_arguments(object, definition.arguments())
}

fn routine_identity_name_from_arguments(
    object: &CatalogObject,
    arguments: &[crate::RoutineArgument],
) -> String {
    use std::fmt::Write;
    let mut key = object.name().normalized().as_str().to_owned();
    for argument in arguments
        .iter()
        .filter(|argument| argument.mode() != crate::ArgumentMode::Out)
    {
        let data_type = argument.data_type();
        write!(
            key,
            "#{:04x}:{}:{}:",
            data_type.descriptor_marker(),
            data_type.parameter_1(),
            data_type.parameter_2()
        )
        .expect("writing routine identity to String cannot fail");
        if let Some(type_id) = data_type.type_object_id() {
            write!(key, "{type_id}").expect("writing routine identity to String cannot fail");
        }
    }
    key
}

fn invalid_object<T>(id: ObjectId, detail: &'static str) -> CatalogResult<T> {
    Err(CatalogError::InvalidCatalogObject {
        id: id.to_string(),
        detail,
    })
}

fn illegal_edge<T>(edge: CatalogEdge, detail: &'static str) -> CatalogResult<T> {
    Err(CatalogError::IllegalCatalogEdge {
        source: edge.source_object_id().to_string(),
        target: edge.target_object_id().to_string(),
        kind: edge.kind().name(),
        detail,
    })
}

#[cfg(test)]
mod tests {
    use radixdb_core::DataType;

    use super::*;
    use crate::{
        AccessMethod, CatalogDataType, CatalogName, ColumnPayload, HnswDistanceMetric,
        HnswParameters, IndexPayload, NamespacePayload, TablePayload, ViewPayload,
    };

    struct Fixture {
        objects: Vec<CatalogObject>,
        edges: Vec<CatalogEdge>,
        table: ObjectId,
        column: ObjectId,
        view: ObjectId,
    }

    fn object(
        id: ObjectId,
        namespace: Option<ObjectId>,
        parent: Option<ObjectId>,
        name: &str,
        payload: CatalogPayload,
    ) -> CatalogObject {
        CatalogObject::new(
            id,
            namespace,
            parent,
            ObjectId::BOOTSTRAP_OWNER,
            CatalogName::new(name).unwrap(),
            1,
            payload,
        )
        .unwrap()
    }

    fn fixture() -> Fixture {
        let namespace = ObjectId::BOOTSTRAP_NAMESPACE;
        let table = ObjectId::new();
        let column = ObjectId::new();
        let constraint = ObjectId::new();
        let index = ObjectId::new();
        let view = ObjectId::new();
        let objects = vec![
            object(
                namespace,
                None,
                None,
                "public",
                CatalogPayload::Namespace(NamespacePayload::new()),
            ),
            object(
                table,
                Some(namespace),
                Some(namespace),
                "messages",
                CatalogPayload::Table(
                    TablePayload::new(
                        vec![column],
                        vec![constraint],
                        vec![index],
                        Some(constraint),
                    )
                    .unwrap(),
                ),
            ),
            object(
                column,
                Some(namespace),
                Some(table),
                "id",
                CatalogPayload::Column(
                    ColumnPayload::new(
                        0,
                        CatalogDataType::scalar(DataType::Integer).unwrap(),
                        false,
                        None,
                        None,
                    )
                    .unwrap(),
                ),
            ),
            object(
                constraint,
                Some(namespace),
                Some(table),
                "messages_pkey",
                CatalogPayload::Constraint(ConstraintPayload::primary_key(vec![column]).unwrap()),
            ),
            object(
                index,
                Some(namespace),
                Some(table),
                "messages_pkey_idx",
                CatalogPayload::Index(
                    IndexPayload::new(AccessMethod::Btree, true, vec![column], vec![], None, None)
                        .unwrap(),
                ),
            ),
            object(
                view,
                Some(namespace),
                Some(namespace),
                "message_ids",
                CatalogPayload::View(
                    ViewPayload::new("SELECT id FROM messages", vec![table], [1; 32]).unwrap(),
                ),
            ),
        ];
        let edges = vec![
            CatalogEdge::new(namespace, table, EdgeKind::Contains, 0),
            CatalogEdge::new(namespace, view, EdgeKind::Contains, 1),
            CatalogEdge::new(table, column, EdgeKind::Contains, 0),
            CatalogEdge::new(table, constraint, EdgeKind::Contains, 1),
            CatalogEdge::new(table, index, EdgeKind::Contains, 2),
            CatalogEdge::new(index, constraint, EdgeKind::DependsOn, 0),
            CatalogEdge::new(view, table, EdgeKind::References, 0),
        ];
        Fixture {
            objects,
            edges,
            table,
            column,
            view,
        }
    }

    #[test]
    fn shuffled_two_pass_load_and_reverse_traversal_are_stable() {
        let mut fixture = fixture();
        fixture.objects.reverse();
        fixture.edges.reverse();
        let graph = CatalogGraph::build(fixture.objects, fixture.edges).unwrap();
        assert_eq!(graph.objects().len(), 6);
        assert!(graph
            .children(fixture.table)
            .any(|item| item.id() == fixture.column));
        assert!(graph
            .dependents(fixture.table)
            .any(|item| item.id() == fixture.view));
        assert!(graph.edges().is_sorted());
    }

    #[test]
    fn relation_name_collision_is_rejected_after_normalization() {
        let mut fixture = fixture();
        let namespace = ObjectId::BOOTSTRAP_NAMESPACE;
        let colliding_view = ObjectId::new();
        // The SQL frontend supplies `messages` for an unquoted identifier and
        // preserves `MESSAGES` as the display spelling of a quoted one. Both
        // intentionally share the same lowercase-NFC durable identity.
        fixture.objects.push(object(
            colliding_view,
            Some(namespace),
            Some(namespace),
            "MESSAGES",
            CatalogPayload::View(ViewPayload::new("SELECT 1", vec![], [0; 32]).unwrap()),
        ));
        fixture.edges.push(CatalogEdge::new(
            namespace,
            colliding_view,
            EdgeKind::Contains,
            2,
        ));
        assert!(matches!(
            CatalogGraph::build(fixture.objects, fixture.edges),
            Err(CatalogError::DuplicateCatalogName { .. })
        ));
    }

    #[test]
    fn index_constraint_and_hnsw_type_links_fail_closed() {
        let mut mismatched_constraint = fixture();
        let index_position = mismatched_constraint
            .objects
            .iter()
            .position(|object| object.kind() == ObjectKind::Index)
            .unwrap();
        let index = mismatched_constraint.objects[index_position].clone();
        mismatched_constraint.objects[index_position] = object(
            index.id(),
            index.namespace_id(),
            index.parent_id(),
            index.name().display().as_str(),
            CatalogPayload::Index(
                IndexPayload::new(
                    AccessMethod::Btree,
                    false,
                    vec![mismatched_constraint.column],
                    vec![],
                    None,
                    None,
                )
                .unwrap(),
            ),
        );
        assert!(matches!(
            CatalogGraph::build(mismatched_constraint.objects, mismatched_constraint.edges),
            Err(CatalogError::InvalidCatalogObject {
                detail: "constraint-owned index does not match its key constraint",
                ..
            })
        ));

        let mut non_vector_hnsw = fixture();
        let index_position = non_vector_hnsw
            .objects
            .iter()
            .position(|object| object.kind() == ObjectKind::Index)
            .unwrap();
        let index = non_vector_hnsw.objects[index_position].clone();
        non_vector_hnsw.objects[index_position] = object(
            index.id(),
            index.namespace_id(),
            index.parent_id(),
            index.name().display().as_str(),
            CatalogPayload::Index(
                IndexPayload::new_hnsw(
                    non_vector_hnsw.column,
                    vec![],
                    HnswParameters::new(16, 200, 64, HnswDistanceMetric::L2).unwrap(),
                )
                .unwrap(),
            ),
        );
        non_vector_hnsw.edges.retain(|edge| {
            !(edge.source_object_id() == index.id() && edge.kind() == EdgeKind::DependsOn)
        });
        assert!(matches!(
            CatalogGraph::build(non_vector_hnsw.objects, non_vector_hnsw.edges),
            Err(CatalogError::InvalidCatalogObject {
                detail: "HNSW key is not a VECTOR column",
                ..
            })
        ));
    }

    #[test]
    fn missing_parent_edge_and_illegal_edge_fail_closed() {
        let mut missing_parent = fixture();
        missing_parent.edges.retain(|edge| {
            !(edge.kind() == EdgeKind::Contains && edge.target_object_id() == missing_parent.column)
        });
        assert!(CatalogGraph::build(missing_parent.objects, missing_parent.edges).is_err());

        let fixture = fixture();
        let mut edges = fixture.edges;
        edges.push(CatalogEdge::new(
            fixture.column,
            fixture.table,
            EdgeKind::DependsOn,
            0,
        ));
        assert!(matches!(
            CatalogGraph::build(fixture.objects, edges),
            Err(CatalogError::IllegalCatalogEdge { .. })
        ));
    }

    #[test]
    fn missing_target_and_non_bootstrap_owner_are_explicit_errors() {
        let fixture = fixture();
        let mut edges = fixture.edges.clone();
        edges.push(CatalogEdge::new(
            fixture.view,
            ObjectId::new(),
            EdgeKind::References,
            1,
        ));
        assert!(matches!(
            CatalogGraph::build(fixture.objects.clone(), edges),
            Err(CatalogError::MissingCatalogObject {
                role: "edge target",
                ..
            })
        ));

        let mut objects = fixture.objects;
        let column_index = objects
            .iter()
            .position(|item| item.id() == fixture.column)
            .unwrap();
        let CatalogPayload::Column(payload) = objects[column_index].payload().clone() else {
            unreachable!()
        };
        objects[column_index] = CatalogObject::new(
            fixture.column,
            Some(ObjectId::BOOTSTRAP_NAMESPACE),
            Some(fixture.table),
            ObjectId::new(),
            CatalogName::new("id").unwrap(),
            1,
            CatalogPayload::Column(payload),
        )
        .unwrap();
        assert!(matches!(
            CatalogGraph::build(objects, fixture.edges),
            Err(CatalogError::InvalidCatalogObject { .. })
        ));
    }

    #[test]
    fn view_dependency_cycle_is_rejected_deterministically() {
        let mut fixture = fixture();
        let namespace = ObjectId::BOOTSTRAP_NAMESPACE;
        let second_view = ObjectId::new();
        let first_index = fixture
            .objects
            .iter()
            .position(|item| item.id() == fixture.view)
            .unwrap();
        fixture.objects[first_index] = object(
            fixture.view,
            Some(namespace),
            Some(namespace),
            "message_ids",
            CatalogPayload::View(
                ViewPayload::new("SELECT * FROM second_view", vec![second_view], [1; 32]).unwrap(),
            ),
        );
        fixture.objects.push(object(
            second_view,
            Some(namespace),
            Some(namespace),
            "second_view",
            CatalogPayload::View(
                ViewPayload::new("SELECT * FROM message_ids", vec![fixture.view], [2; 32]).unwrap(),
            ),
        ));
        fixture.edges.retain(|edge| {
            !(edge.source_object_id() == fixture.view && edge.kind().is_dependency())
        });
        fixture.edges.extend([
            CatalogEdge::new(namespace, second_view, EdgeKind::Contains, 2),
            CatalogEdge::new(fixture.view, second_view, EdgeKind::DependsOn, 0),
            CatalogEdge::new(second_view, fixture.view, EdgeKind::DependsOn, 0),
        ]);
        let mut reversed_objects = fixture.objects.clone();
        let mut reversed_edges = fixture.edges.clone();
        reversed_objects.reverse();
        reversed_edges.reverse();
        let forward = CatalogGraph::build(fixture.objects, fixture.edges).unwrap_err();
        let reverse = CatalogGraph::build(reversed_objects, reversed_edges).unwrap_err();
        assert!(matches!(
            forward,
            CatalogError::CatalogDependencyCycle { .. }
        ));
        assert_eq!(forward, reverse);
    }

    fn foreign_key_fixture(
        local_type: DataType,
        referenced_type: DataType,
        target_unique: bool,
        local_nullable: bool,
        on_delete: crate::ForeignKeyAction,
    ) -> (Vec<CatalogObject>, Vec<CatalogEdge>) {
        let namespace = ObjectId::BOOTSTRAP_NAMESPACE;
        let parent = ObjectId::new();
        let parent_column = ObjectId::new();
        let parent_key = ObjectId::new();
        let child = ObjectId::new();
        let child_column = ObjectId::new();
        let foreign_key = ObjectId::new();
        let parent_constraints = target_unique.then_some(parent_key).into_iter().collect();
        let objects = vec![
            object(
                namespace,
                None,
                None,
                "public",
                CatalogPayload::Namespace(NamespacePayload::new()),
            ),
            object(
                parent,
                Some(namespace),
                Some(namespace),
                "parent",
                CatalogPayload::Table(
                    TablePayload::new(vec![parent_column], parent_constraints, vec![], None)
                        .unwrap(),
                ),
            ),
            object(
                parent_column,
                Some(namespace),
                Some(parent),
                "id",
                CatalogPayload::Column(
                    ColumnPayload::new(
                        0,
                        CatalogDataType::scalar(referenced_type).unwrap(),
                        false,
                        None,
                        None,
                    )
                    .unwrap(),
                ),
            ),
            object(
                child,
                Some(namespace),
                Some(namespace),
                "child",
                CatalogPayload::Table(
                    TablePayload::new(vec![child_column], vec![foreign_key], vec![], None).unwrap(),
                ),
            ),
            object(
                child_column,
                Some(namespace),
                Some(child),
                "parent_id",
                CatalogPayload::Column(
                    ColumnPayload::new(
                        0,
                        CatalogDataType::scalar(local_type).unwrap(),
                        local_nullable,
                        None,
                        None,
                    )
                    .unwrap(),
                ),
            ),
            object(
                foreign_key,
                Some(namespace),
                Some(child),
                "fk_child_parent",
                CatalogPayload::Constraint(
                    ConstraintPayload::foreign_key(
                        vec![child_column],
                        parent,
                        vec![parent_column],
                        crate::ForeignKeyMatch::Simple,
                        crate::ForeignKeyAction::NoAction,
                        on_delete,
                    )
                    .unwrap(),
                ),
            ),
        ];
        let mut edges = vec![
            CatalogEdge::new(namespace, parent, EdgeKind::Contains, 0),
            CatalogEdge::new(parent, parent_column, EdgeKind::Contains, 0),
            CatalogEdge::new(namespace, child, EdgeKind::Contains, 1),
            CatalogEdge::new(child, child_column, EdgeKind::Contains, 0),
            CatalogEdge::new(child, foreign_key, EdgeKind::Contains, 1),
            CatalogEdge::new(foreign_key, parent, EdgeKind::References, 0),
            CatalogEdge::new(foreign_key, parent_column, EdgeKind::References, 1),
        ];
        if target_unique {
            let parent_key_object = object(
                parent_key,
                Some(namespace),
                Some(parent),
                "uq_parent_id",
                CatalogPayload::Constraint(ConstraintPayload::unique(vec![parent_column]).unwrap()),
            );
            let insert_at = 3;
            let mut objects = objects;
            objects.insert(insert_at, parent_key_object);
            edges.push(CatalogEdge::new(parent, parent_key, EdgeKind::Contains, 1));
            return (objects, edges);
        }
        (objects, edges)
    }

    fn foreign_key_index_fixture(
        unique: bool,
        predicate_sql: Option<&str>,
    ) -> (Vec<CatalogObject>, Vec<CatalogEdge>) {
        let (mut objects, mut edges) = foreign_key_fixture(
            DataType::Integer,
            DataType::Integer,
            false,
            true,
            crate::ForeignKeyAction::NoAction,
        );
        let namespace = ObjectId::BOOTSTRAP_NAMESPACE;
        let parent = objects
            .iter()
            .find(|object| object.name().normalized().as_str() == "parent")
            .expect("parent fixture object exists")
            .id();
        let parent_column = objects
            .iter()
            .find(|object| {
                object.parent_id() == Some(parent) && object.kind() == ObjectKind::Column
            })
            .expect("parent fixture column exists")
            .id();
        let index = ObjectId::new();
        let parent_position = objects
            .iter()
            .position(|object| object.id() == parent)
            .expect("parent fixture object position exists");
        objects[parent_position] = object(
            parent,
            Some(namespace),
            Some(namespace),
            "parent",
            CatalogPayload::Table(
                TablePayload::new(vec![parent_column], vec![], vec![index], None).unwrap(),
            ),
        );
        objects.push(object(
            index,
            Some(namespace),
            Some(parent),
            "parent_id_uq",
            CatalogPayload::Index(
                IndexPayload::new(
                    AccessMethod::Btree,
                    unique,
                    vec![parent_column],
                    vec![],
                    None,
                    predicate_sql.map(str::to_owned),
                )
                .unwrap(),
            ),
        ));
        edges.push(CatalogEdge::new(parent, index, EdgeKind::Contains, 1));
        (objects, edges)
    }

    #[test]
    fn foreign_key_type_target_and_action_invariants_fail_closed() {
        let (objects, edges) = foreign_key_fixture(
            DataType::Integer,
            DataType::Integer,
            true,
            true,
            crate::ForeignKeyAction::SetNull,
        );
        assert!(CatalogGraph::build(objects, edges).is_ok());

        for (objects, edges) in [
            foreign_key_fixture(
                DataType::Text,
                DataType::Integer,
                true,
                true,
                crate::ForeignKeyAction::NoAction,
            ),
            foreign_key_fixture(
                DataType::Integer,
                DataType::Integer,
                false,
                true,
                crate::ForeignKeyAction::NoAction,
            ),
            foreign_key_fixture(
                DataType::Integer,
                DataType::Integer,
                true,
                false,
                crate::ForeignKeyAction::SetNull,
            ),
        ] {
            assert!(matches!(
                CatalogGraph::build(objects, edges),
                Err(CatalogError::InvalidCatalogObject { .. })
            ));
        }
    }

    #[test]
    fn foreign_key_accepts_only_a_full_standalone_unique_index() {
        let (objects, edges) = foreign_key_index_fixture(true, None);
        assert!(CatalogGraph::build(objects, edges).is_ok());

        for (objects, edges) in [
            foreign_key_index_fixture(false, None),
            foreign_key_index_fixture(true, Some("id > 0")),
        ] {
            assert!(matches!(
                CatalogGraph::build(objects, edges),
                Err(CatalogError::InvalidCatalogObject {
                    detail: "foreign key target is neither PRIMARY KEY nor UNIQUE",
                    ..
                })
            ));
        }
    }

    #[test]
    fn primary_key_and_not_null_constraints_require_non_nullable_columns() {
        let mut primary = fixture();
        let column_index = primary
            .objects
            .iter()
            .position(|object| object.id() == primary.column)
            .unwrap();
        primary.objects[column_index] = object(
            primary.column,
            Some(ObjectId::BOOTSTRAP_NAMESPACE),
            Some(primary.table),
            "id",
            CatalogPayload::Column(
                ColumnPayload::new(
                    0,
                    CatalogDataType::scalar(DataType::Integer).unwrap(),
                    true,
                    None,
                    None,
                )
                .unwrap(),
            ),
        );
        assert!(matches!(
            CatalogGraph::build(primary.objects, primary.edges),
            Err(CatalogError::InvalidCatalogObject { .. })
        ));

        let namespace = ObjectId::BOOTSTRAP_NAMESPACE;
        let table = ObjectId::new();
        let column = ObjectId::new();
        let not_null = ObjectId::new();
        let objects = vec![
            object(
                namespace,
                None,
                None,
                "public",
                CatalogPayload::Namespace(NamespacePayload::new()),
            ),
            object(
                table,
                Some(namespace),
                Some(namespace),
                "nullable_table",
                CatalogPayload::Table(
                    TablePayload::new(vec![column], vec![not_null], vec![], None).unwrap(),
                ),
            ),
            object(
                column,
                Some(namespace),
                Some(table),
                "value",
                CatalogPayload::Column(
                    ColumnPayload::new(
                        0,
                        CatalogDataType::scalar(DataType::Integer).unwrap(),
                        true,
                        None,
                        None,
                    )
                    .unwrap(),
                ),
            ),
            object(
                not_null,
                Some(namespace),
                Some(table),
                "nn_nullable_table_value",
                CatalogPayload::Constraint(ConstraintPayload::not_null(column)),
            ),
        ];
        let edges = vec![
            CatalogEdge::new(namespace, table, EdgeKind::Contains, 0),
            CatalogEdge::new(table, column, EdgeKind::Contains, 0),
            CatalogEdge::new(table, not_null, EdgeKind::Contains, 1),
        ];
        assert!(matches!(
            CatalogGraph::build(objects, edges),
            Err(CatalogError::InvalidCatalogObject { .. })
        ));
    }
}
