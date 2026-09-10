use radixdb_catalog::{
    CatalogGeneration, CatalogObject, CatalogPayload, ConstraintPayload, ObjectId, ObjectKind,
    TablePayload,
};
use radixdb_core::{Error, Result};

use super::transaction::catalog_argument;

/// Typed read-only table view pinned to one immutable catalog generation.
#[derive(Debug, Clone, Copy)]
pub struct TableCatalog<'generation> {
    generation: &'generation CatalogGeneration,
    table: &'generation CatalogObject,
    payload: &'generation TablePayload,
}

impl<'generation> TableCatalog<'generation> {
    pub fn load(generation: &'generation CatalogGeneration, table_name: &str) -> Result<Self> {
        let table = generation
            .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, table_name)
            .map_err(catalog_argument)?
            .filter(|object| object.kind() == ObjectKind::Table)
            .ok_or_else(|| Error::TableNotFound(table_name.to_owned()))?;
        let CatalogPayload::Table(payload) = table.payload() else {
            return Err(Error::internal(
                "catalog admitted a table with a non-table payload",
            ));
        };
        Ok(Self {
            generation,
            table,
            payload,
        })
    }

    pub const fn object(&self) -> &'generation CatalogObject {
        self.table
    }

    pub const fn id(&self) -> ObjectId {
        self.table.id()
    }

    pub const fn payload(&self) -> &'generation TablePayload {
        self.payload
    }

    pub fn columns(&self) -> impl ExactSizeIterator<Item = &'generation CatalogObject> + '_ {
        self.payload.column_ids().iter().map(|id| {
            self.generation
                .object(*id)
                .expect("validated table column ID must resolve")
        })
    }

    pub fn constraints(&self) -> impl ExactSizeIterator<Item = &'generation CatalogObject> + '_ {
        self.payload.constraint_ids().iter().map(|id| {
            self.generation
                .object(*id)
                .expect("validated table constraint ID must resolve")
        })
    }

    pub fn indexes(&self) -> impl ExactSizeIterator<Item = &'generation CatalogObject> + '_ {
        self.payload.index_ids().iter().map(|id| {
            self.generation
                .object(*id)
                .expect("validated table index ID must resolve")
        })
    }

    pub fn column(&self, name: &str) -> Result<&'generation CatalogObject> {
        self.generation
            .find_column(self.table.id(), name)
            .map_err(catalog_argument)?
            .ok_or_else(|| Error::ColumnNotFound(name.to_owned()))
    }

    pub fn constraint(&self, name: &str) -> Result<&'generation CatalogObject> {
        self.generation
            .find_constraint(self.table.id(), name)
            .map_err(catalog_argument)?
            .ok_or_else(|| {
                Error::InvalidArgument(format!(
                    "constraint '{name}' does not exist on table '{}'",
                    self.table.name().display().as_str()
                ))
            })
    }

    pub fn index(&self, name: &str) -> Result<&'generation CatalogObject> {
        self.generation
            .find_index(
                self.table
                    .namespace_id()
                    .expect("validated table must have a namespace"),
                name,
            )
            .map_err(catalog_argument)?
            .filter(|index| index.parent_id() == Some(self.table.id()))
            .ok_or_else(|| Error::IndexNotFound(name.to_owned()))
    }

    pub fn primary_key(&self) -> Option<&'generation CatalogObject> {
        self.payload
            .primary_key_constraint_id()
            .and_then(|id| self.generation.object(id))
    }

    pub fn foreign_keys(
        &self,
    ) -> impl Iterator<Item = (&'generation CatalogObject, &'generation ConstraintPayload)> + '_
    {
        self.constraints().filter_map(|object| {
            let CatalogPayload::Constraint(payload @ ConstraintPayload::ForeignKey { .. }) =
                object.payload()
            else {
                return None;
            };
            Some((object, payload))
        })
    }
}
