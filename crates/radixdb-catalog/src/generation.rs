use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use crate::{
    CatalogError, CatalogGraph, CatalogMutationSet, CatalogName, CatalogObject, CatalogPack,
    CatalogPackMeta, CatalogResult, ObjectClass, ObjectId, ObjectKind,
};

type NameKey = (Option<ObjectId>, ObjectClass, String);

/// One completely validated immutable runtime catalog generation.
#[derive(Debug)]
pub struct CatalogGeneration {
    format_minor: u16,
    meta: CatalogPackMeta,
    graph: CatalogGraph,
    names: BTreeMap<NameKey, ObjectId>,
    kinds: BTreeMap<ObjectKind, Vec<ObjectId>>,
    body_sha256: Option<[u8; 32]>,
}

impl CatalogGeneration {
    pub fn new(meta: CatalogPackMeta, graph: CatalogGraph) -> Self {
        let format_minor = graph.required_format_minor();
        Self::from_validated_parts(format_minor, meta, graph, None)
    }

    /// Build a validated generation while preserving a previously published
    /// catalog minor. Catalog formats are monotonic even when the last object
    /// requiring a newer minor is dropped: its DROP mutation and every later
    /// checkpoint must remain decodable by the same binary generation.
    pub fn new_for_minor(
        format_minor: u16,
        meta: CatalogPackMeta,
        graph: CatalogGraph,
    ) -> CatalogResult<Self> {
        if format_minor > crate::LATEST_CATALOG_MINOR {
            return Err(CatalogError::UnsupportedCatalogMinor {
                major: 6,
                minor: format_minor,
            });
        }
        if graph.required_format_minor() > format_minor {
            return Err(CatalogError::InvalidCatalogMutation {
                detail: "catalog graph requires a newer minor",
            });
        }
        Ok(Self::from_validated_parts(format_minor, meta, graph, None))
    }

    pub fn from_pack(pack: CatalogPack) -> Self {
        let (format_minor, meta, graph, body_sha256) = pack.into_parts();
        Self::from_validated_parts(format_minor, meta, graph, Some(body_sha256))
    }

    fn from_validated_parts(
        format_minor: u16,
        meta: CatalogPackMeta,
        graph: CatalogGraph,
        body_sha256: Option<[u8; 32]>,
    ) -> Self {
        let mut names = BTreeMap::new();
        let mut kinds = BTreeMap::<ObjectKind, Vec<ObjectId>>::new();
        for object in graph.objects() {
            let key = name_key(object);
            let previous = names.insert(key, object.id());
            debug_assert!(previous.is_none(), "CatalogGraph admitted a duplicate name");
            kinds.entry(object.kind()).or_default().push(object.id());
        }
        Self {
            format_minor,
            meta,
            graph,
            names,
            kinds,
            body_sha256,
        }
    }

    pub const fn format_minor(&self) -> u16 {
        self.format_minor
    }

    pub const fn meta(&self) -> CatalogPackMeta {
        self.meta
    }

    pub const fn graph(&self) -> &CatalogGraph {
        &self.graph
    }

    pub const fn body_sha256(&self) -> Option<&[u8; 32]> {
        self.body_sha256.as_ref()
    }

    pub fn object(&self, id: ObjectId) -> Option<&CatalogObject> {
        self.graph.object(id)
    }

    pub fn objects_of_kind(
        &self,
        kind: ObjectKind,
    ) -> impl ExactSizeIterator<Item = &CatalogObject> {
        let ids = self.kinds.get(&kind).map(Vec::as_slice).unwrap_or_default();
        ids.iter().map(|id| {
            self.graph
                .object(*id)
                .expect("generation kind index references admitted object")
        })
    }

    pub fn find_namespace(
        &self,
        parent_namespace_id: Option<ObjectId>,
        name: &str,
    ) -> CatalogResult<Option<&CatalogObject>> {
        self.find_name(parent_namespace_id, ObjectClass::Namespace, name)
    }

    pub fn find_relation(
        &self,
        namespace_id: ObjectId,
        name: &str,
    ) -> CatalogResult<Option<&CatalogObject>> {
        self.find_name(Some(namespace_id), ObjectClass::Relation, name)
    }

    pub fn find_column(
        &self,
        table_id: ObjectId,
        name: &str,
    ) -> CatalogResult<Option<&CatalogObject>> {
        self.find_name(Some(table_id), ObjectClass::Column, name)
    }

    pub fn find_constraint(
        &self,
        table_id: ObjectId,
        name: &str,
    ) -> CatalogResult<Option<&CatalogObject>> {
        self.find_name(Some(table_id), ObjectClass::Constraint, name)
    }

    pub fn find_index(
        &self,
        namespace_id: ObjectId,
        name: &str,
    ) -> CatalogResult<Option<&CatalogObject>> {
        self.find_name(Some(namespace_id), ObjectClass::Index, name)
    }

    pub fn find_extension(&self, name: &str) -> CatalogResult<Option<&CatalogObject>> {
        self.find_name(None, ObjectClass::Extension, name)
    }

    pub fn find_external_type(
        &self,
        namespace_id: ObjectId,
        name: &str,
    ) -> CatalogResult<Option<&CatalogObject>> {
        self.find_name(Some(namespace_id), ObjectClass::Type, name)
    }

    pub fn find_operator(
        &self,
        namespace_id: ObjectId,
        symbol: &str,
        left: Option<crate::CatalogDataType>,
        right: Option<crate::CatalogDataType>,
    ) -> CatalogResult<Option<&CatalogObject>> {
        let normalized = CatalogName::new(symbol)?.normalized().as_str().to_owned();
        let key = operator_key_name(normalized, left, right);
        Ok(self
            .names
            .get(&(Some(namespace_id), ObjectClass::Operator, key))
            .and_then(|id| self.graph.object(*id)))
    }

    pub fn find_operator_class(
        &self,
        namespace_id: ObjectId,
        name: &str,
    ) -> CatalogResult<Option<&CatalogObject>> {
        self.find_name(Some(namespace_id), ObjectClass::OperatorClass, name)
    }

    pub fn find_planner_support(
        &self,
        namespace_id: ObjectId,
        name: &str,
    ) -> CatalogResult<Option<&CatalogObject>> {
        self.find_name(Some(namespace_id), ObjectClass::PlannerSupport, name)
    }

    pub fn find_routine(
        &self,
        namespace_id: ObjectId,
        kind: ObjectKind,
        name: &str,
        input_types: &[crate::CatalogDataType],
    ) -> CatalogResult<Option<&CatalogObject>> {
        if !matches!(kind, ObjectKind::Function | ObjectKind::Procedure) {
            return Err(CatalogError::InvalidCatalogObject {
                id: namespace_id.to_string(),
                detail: "routine lookup kind is not Function or Procedure",
            });
        }
        let normalized = CatalogName::new(name)?.normalized().as_str().to_owned();
        let key = routine_key_name(normalized, input_types.iter().copied());
        Ok(self
            .names
            .get(&(Some(namespace_id), kind.object_class(), key))
            .and_then(|id| self.graph.object(*id)))
    }

    fn find_name(
        &self,
        scope: Option<ObjectId>,
        class: ObjectClass,
        name: &str,
    ) -> CatalogResult<Option<&CatalogObject>> {
        let normalized = CatalogName::new(name)?.normalized().as_str().to_owned();
        Ok(self
            .names
            .get(&(scope, class, normalized))
            .and_then(|id| self.graph.object(*id)))
    }
}

/// Single publication point for immutable generations.
///
/// Readers clone one `Arc` under a short read lock. Publication swaps the Arc
/// under a short write lock; already pinned readers retain their old complete
/// generation and observe no in-place mutation.
#[derive(Debug)]
pub struct CatalogPublisher {
    current: RwLock<Arc<CatalogGeneration>>,
}

/// Fully validated immutable successor tied to one exact source generation.
///
/// Construction is private to `CatalogMutationSet::prepare`; publication uses
/// an in-lock compare-and-swap against all expected source identities.
#[derive(Debug)]
pub struct PreparedCatalogMutation {
    expected_database_id: [u8; 16],
    expected_catalog_id: [u8; 16],
    expected_catalog_generation: u64,
    next: Arc<CatalogGeneration>,
}

impl PreparedCatalogMutation {
    pub(crate) fn new(
        mutation_set: &CatalogMutationSet,
        current: &CatalogGeneration,
        next_meta: CatalogPackMeta,
        graph: CatalogGraph,
    ) -> CatalogResult<Self> {
        let current_meta = current.meta();
        if next_meta.database_id() != current_meta.database_id() {
            return Err(CatalogError::InvalidCatalogMutation {
                detail: "successor changes database identity",
            });
        }
        let next_generation = mutation_set
            .expected_catalog_generation()
            .checked_add(1)
            .ok_or(CatalogError::InvalidCatalogMutation {
                detail: "catalog generation overflow",
            })?;
        if next_meta.catalog_generation() != next_generation {
            return Err(CatalogError::InvalidCatalogMutation {
                detail: "successor generation is not expected generation plus one",
            });
        }
        if next_meta.snapshot_lsn() < current_meta.snapshot_lsn() {
            return Err(CatalogError::InvalidCatalogMutation {
                detail: "successor snapshot LSN moves backwards",
            });
        }
        Ok(Self {
            expected_database_id: *mutation_set.expected_database_id(),
            expected_catalog_id: *mutation_set.expected_catalog_id(),
            expected_catalog_generation: mutation_set.expected_catalog_generation(),
            next: Arc::new(CatalogGeneration::new(next_meta, graph)),
        })
    }

    pub const fn next(&self) -> &Arc<CatalogGeneration> {
        &self.next
    }
}

impl CatalogPublisher {
    pub fn new(initial: Arc<CatalogGeneration>) -> Self {
        Self {
            current: RwLock::new(initial),
        }
    }

    pub fn pin(&self) -> CatalogResult<Arc<CatalogGeneration>> {
        self.current
            .read()
            .map(|current| Arc::clone(&current))
            .map_err(|_| CatalogError::CatalogPublicationUnavailable)
    }

    #[cfg(test)]
    fn publish(&self, next: Arc<CatalogGeneration>) -> CatalogResult<Arc<CatalogGeneration>> {
        let mut current = self
            .current
            .write()
            .map_err(|_| CatalogError::CatalogPublicationUnavailable)?;
        let previous_meta = current.meta();
        let next_meta = next.meta();
        if previous_meta.database_id() != next_meta.database_id() {
            return Err(CatalogError::InvalidCatalogPublication {
                detail: "database identity changed",
            });
        }
        if next_meta.catalog_generation() <= previous_meta.catalog_generation() {
            return Err(CatalogError::InvalidCatalogPublication {
                detail: "catalog generation did not advance",
            });
        }
        if next_meta.snapshot_lsn() < previous_meta.snapshot_lsn() {
            return Err(CatalogError::InvalidCatalogPublication {
                detail: "catalog snapshot LSN moved backwards",
            });
        }
        Ok(std::mem::replace(&mut *current, next))
    }

    pub fn publish_prepared(
        &self,
        prepared: PreparedCatalogMutation,
    ) -> CatalogResult<Arc<CatalogGeneration>> {
        let mut current = self
            .current
            .write()
            .map_err(|_| CatalogError::CatalogPublicationUnavailable)?;
        let meta = current.meta();
        require_publication_identity(
            "database identity",
            &prepared.expected_database_id,
            &meta.database_id(),
        )?;
        require_publication_identity(
            "catalog identity",
            &prepared.expected_catalog_id,
            &meta.catalog_id(),
        )?;
        if meta.catalog_generation() != prepared.expected_catalog_generation {
            return Err(CatalogError::StaleCatalogMutation {
                field: "catalog generation",
                expected: prepared.expected_catalog_generation.to_string(),
                actual: meta.catalog_generation().to_string(),
            });
        }
        Ok(std::mem::replace(&mut *current, prepared.next))
    }
}

fn require_publication_identity(
    field: &'static str,
    expected: &[u8; 16],
    actual: &[u8; 16],
) -> CatalogResult<()> {
    if expected != actual {
        return Err(CatalogError::StaleCatalogMutation {
            field,
            expected: identity_hex(expected),
            actual: identity_hex(actual),
        });
    }
    Ok(())
}

fn identity_hex(bytes: &[u8; 16]) -> String {
    let mut output = String::with_capacity(32);
    for byte in bytes {
        use std::fmt::Write;
        write!(output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn name_key(object: &CatalogObject) -> NameKey {
    let scope = match object.kind() {
        ObjectKind::Column | ObjectKind::Constraint => object.parent_id(),
        ObjectKind::Trigger => object.parent_id(),
        ObjectKind::Principal | ObjectKind::Role | ObjectKind::AclEntry | ObjectKind::Extension => {
            None
        }
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
    };
    let mut name = object.name().normalized().as_str().to_owned();
    let arguments = match object.payload() {
        crate::CatalogPayload::Function(payload) => Some(payload.arguments()),
        crate::CatalogPayload::Procedure(payload) => Some(payload.definition().arguments()),
        _ => None,
    };
    if let Some(arguments) = arguments {
        name = routine_key_name(
            name,
            arguments
                .iter()
                .filter(|argument| argument.mode() != crate::ArgumentMode::Out)
                .map(crate::RoutineArgument::data_type),
        );
    }
    if let crate::CatalogPayload::Operator(payload) = object.payload() {
        name = operator_key_name(name, payload.left_argument(), payload.right_argument());
    }
    (scope, object.kind().object_class(), name)
}

fn operator_key_name(
    mut name: String,
    left: Option<crate::CatalogDataType>,
    right: Option<crate::CatalogDataType>,
) -> String {
    use std::fmt::Write;
    name.push('(');
    for data_type in [left, right] {
        match data_type {
            Some(value) if value.is_external() => {
                let id = value
                    .type_object_id()
                    .expect("external type has object identity");
                write!(name, "x{}v{}", id, value.parameter_1())
                    .expect("writing to String cannot fail");
            }
            Some(value) => write!(name, "b{}", value.logical_type() as u8)
                .expect("writing to String cannot fail"),
            None => name.push('_'),
        }
        name.push(',');
    }
    name.push(')');
    name
}

fn routine_key_name(
    mut name: String,
    input_types: impl Iterator<Item = crate::CatalogDataType>,
) -> String {
    use std::fmt::Write;
    for data_type in input_types {
        write!(
            name,
            "#{:04x}:{}:{}:",
            data_type.descriptor_marker(),
            data_type.parameter_1(),
            data_type.parameter_2()
        )
        .expect("writing routine identity cannot fail");
        if let Some(type_id) = data_type.type_object_id() {
            write!(name, "{type_id}").expect("writing routine identity cannot fail");
        }
    }
    name
}

#[cfg(test)]
mod tests {
    use radixdb_core::DataType;

    use super::*;
    use crate::{
        CatalogDataType, CatalogEdge, CatalogPayload, ColumnPayload, EdgeKind, NamespacePayload,
        TablePayload,
    };

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

    fn generation(number: u64, with_table: bool) -> Arc<CatalogGeneration> {
        let namespace = ObjectId::BOOTSTRAP_NAMESPACE;
        let mut objects = vec![object(
            namespace,
            None,
            None,
            "public",
            CatalogPayload::Namespace(NamespacePayload::new()),
        )];
        let mut edges = vec![];
        if with_table {
            let table = ObjectId::from_user_bytes([number as u8 + 1; 16]).unwrap();
            let column = ObjectId::from_user_bytes([number as u8 + 20; 16]).unwrap();
            objects.push(object(
                table,
                Some(namespace),
                Some(namespace),
                "Messages",
                CatalogPayload::Table(
                    TablePayload::new(vec![column], vec![], vec![], None).unwrap(),
                ),
            ));
            objects.push(object(
                column,
                Some(namespace),
                Some(table),
                "ID",
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
            ));
            edges.extend([
                CatalogEdge::new(namespace, table, EdgeKind::Contains, 0),
                CatalogEdge::new(table, column, EdgeKind::Contains, 0),
            ]);
        }
        Arc::new(CatalogGeneration::new(
            CatalogPackMeta::new([1; 16], [number as u8; 16], number, number * 10, number).unwrap(),
            CatalogGraph::build(objects, edges).unwrap(),
        ))
    }

    #[test]
    fn pinned_reader_retains_old_generation_after_atomic_publish() {
        let first = generation(1, false);
        let publisher = CatalogPublisher::new(Arc::clone(&first));
        let pinned = publisher.pin().unwrap();
        let second = generation(2, false);
        let replaced = publisher.publish(Arc::clone(&second)).unwrap();

        assert!(Arc::ptr_eq(&pinned, &first));
        assert!(Arc::ptr_eq(&replaced, &first));
        assert_eq!(pinned.meta().catalog_generation(), 1);
        assert!(Arc::ptr_eq(&publisher.pin().unwrap(), &second));
    }

    #[test]
    fn publication_rejects_stale_or_foreign_generation() {
        let publisher = CatalogPublisher::new(generation(2, false));
        assert!(publisher.publish(generation(1, false)).is_err());
        let foreign = Arc::new(CatalogGeneration::new(
            CatalogPackMeta::new([9; 16], [3; 16], 3, 30, 3).unwrap(),
            CatalogGraph::build(
                vec![object(
                    ObjectId::BOOTSTRAP_NAMESPACE,
                    None,
                    None,
                    "public",
                    CatalogPayload::Namespace(NamespacePayload::new()),
                )],
                vec![],
            )
            .unwrap(),
        ));
        assert!(publisher.publish(foreign).is_err());
    }

    #[test]
    fn immutable_lookup_indexes_use_normalized_names_and_kind() {
        let generation = generation(1, true);
        let namespace = ObjectId::BOOTSTRAP_NAMESPACE;
        let table = generation
            .find_relation(namespace, "messages")
            .unwrap()
            .unwrap();
        assert_eq!(table.kind(), ObjectKind::Table);
        assert_eq!(
            generation
                .find_column(table.id(), "id")
                .unwrap()
                .unwrap()
                .kind(),
            ObjectKind::Column
        );
        assert_eq!(generation.objects_of_kind(ObjectKind::Table).len(), 1);
        assert_eq!(generation.objects_of_kind(ObjectKind::View).len(), 0);
    }
}
