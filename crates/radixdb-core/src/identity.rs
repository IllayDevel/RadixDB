//! Runtime identities bound to one schema generation.

/// Create opaque time-ordered bytes for a durable higher-layer identity.
///
/// Core deliberately does not attach catalog or artifact semantics to these
/// bytes. Owners in sibling crates apply their own reserved-range admission.
#[doc(hidden)]
pub fn new_durable_identity_bytes() -> [u8; 16] {
    uuid::Uuid::now_v7().into_bytes()
}

/// Runtime-stable identity of a table inside one engine schema generation.
///
/// The identity is intentionally not persisted. Any DDL changes the schema
/// generation and invalidates previously bound identities; reopen builds a new
/// engine scope and callers bind again from the durable schema catalog.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SchemaTableId {
    scope_id: u64,
    schema_generation: u64,
    table_name_lower: String,
}

impl SchemaTableId {
    /// Construct an identity after authoritative schema binding.
    ///
    /// This is public only because the storage owner lives in a sibling crate.
    /// Applications must obtain identities through the engine binding API.
    #[doc(hidden)]
    pub fn new(scope_id: u64, schema_generation: u64, table_name_lower: String) -> Self {
        Self {
            scope_id,
            schema_generation,
            table_name_lower,
        }
    }

    pub fn scope_id(&self) -> u64 {
        self.scope_id
    }

    pub fn schema_generation(&self) -> u64 {
        self.schema_generation
    }

    pub fn table_name(&self) -> &str {
        &self.table_name_lower
    }
}

/// Runtime-stable identity of a column inside one bound table generation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SchemaColumnId {
    table: SchemaTableId,
    ordinal: usize,
}

impl SchemaColumnId {
    /// Construct an identity after authoritative column binding.
    ///
    /// This is public only because the storage owner lives in a sibling crate.
    /// Applications must obtain identities through the engine binding API.
    #[doc(hidden)]
    pub fn new(table: SchemaTableId, ordinal: usize) -> Self {
        Self { table, ordinal }
    }

    pub fn table(&self) -> &SchemaTableId {
        &self.table
    }

    pub fn ordinal(&self) -> usize {
        self.ordinal
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identities_preserve_scope_generation_name_and_ordinal() {
        let table = SchemaTableId::new(7, 11, "users".to_owned());
        let column = SchemaColumnId::new(table.clone(), 3);

        assert_eq!(table.scope_id(), 7);
        assert_eq!(table.schema_generation(), 11);
        assert_eq!(table.table_name(), "users");
        assert_eq!(column.table(), &table);
        assert_eq!(column.ordinal(), 3);
    }

    #[test]
    fn durable_identity_bytes_are_uuid_v7_and_distinct() {
        let first = new_durable_identity_bytes();
        let second = new_durable_identity_bytes();
        assert_ne!(first, second);
        assert_eq!(first[6] >> 4, 7);
        assert_eq!(second[6] >> 4, 7);
    }
}
