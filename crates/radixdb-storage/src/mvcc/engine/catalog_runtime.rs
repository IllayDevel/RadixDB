use std::sync::Arc;

use radixdb_catalog::{CatalogGeneration, ObjectKind};
use radixdb_core::{Error, Result, Schema};

use crate::mvcc::{IndexDefinition, ViewDefinition};

/// SQL-bound runtime state derived from one immutable catalog generation.
///
/// Storage owns installation, but not SQL parsing or expression binding. The
/// executor supplies this complete value before any DML WAL entry is replayed.
#[derive(Debug)]
pub struct CatalogRuntime {
    tables: Vec<CatalogRuntimeTable>,
    views: Vec<ViewDefinition>,
}

impl CatalogRuntime {
    pub fn new(
        mut tables: Vec<CatalogRuntimeTable>,
        mut views: Vec<ViewDefinition>,
    ) -> Result<Self> {
        tables.sort_unstable_by(|left, right| {
            left.schema
                .table_name_lower
                .cmp(&right.schema.table_name_lower)
        });
        views.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        if tables
            .windows(2)
            .any(|pair| pair[0].schema.table_name_lower == pair[1].schema.table_name_lower)
            || views.windows(2).any(|pair| pair[0].name == pair[1].name)
        {
            return Err(Error::internal(
                "catalog runtime contains duplicate table or view names",
            ));
        }
        Ok(Self { tables, views })
    }

    pub(crate) fn into_parts(self) -> (Vec<CatalogRuntimeTable>, Vec<ViewDefinition>) {
        (self.tables, self.views)
    }
}

#[derive(Debug)]
pub struct CatalogRuntimeTable {
    schema: Schema,
    indexes: Vec<IndexDefinition>,
}

impl CatalogRuntimeTable {
    pub fn new(schema: Schema, indexes: Vec<IndexDefinition>) -> Result<Self> {
        if indexes
            .iter()
            .any(|index| !index.table_name.eq_ignore_ascii_case(&schema.table_name))
        {
            return Err(Error::internal(
                "catalog runtime index belongs to another table",
            ));
        }
        Ok(Self { schema, indexes })
    }

    pub(crate) fn into_parts(self) -> (Schema, Vec<IndexDefinition>) {
        (self.schema, self.indexes)
    }
}

/// Immutable composition boundary from the durable logical catalog to runtime
/// schema, expression and index owners.
///
/// The closure form is intentional: an SQL facade can capture its immutable
/// startup plugin registry and prepare semantic callbacks without teaching
/// storage about the plugin ABI or loader.
type BindCatalogRuntime = dyn Fn(&CatalogGeneration) -> Result<CatalogRuntime> + Send + Sync;

#[derive(Clone)]
pub struct CatalogRuntimeBinder(Arc<BindCatalogRuntime>);

impl CatalogRuntimeBinder {
    pub fn new(
        binder: impl Fn(&CatalogGeneration) -> Result<CatalogRuntime> + Send + Sync + 'static,
    ) -> Self {
        Self(Arc::new(binder))
    }

    pub(crate) fn bind(&self, generation: &CatalogGeneration) -> Result<CatalogRuntime> {
        (self.0)(generation)
    }
}

impl std::fmt::Debug for CatalogRuntimeBinder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("CatalogRuntimeBinder(..)")
    }
}

/// Converts either the historical function binder or a captured immutable
/// binder into the runtime composition port.
#[doc(hidden)]
pub trait IntoCatalogRuntimeBinder {
    fn into_catalog_runtime_binder(self) -> CatalogRuntimeBinder;
}

impl IntoCatalogRuntimeBinder for CatalogRuntimeBinder {
    fn into_catalog_runtime_binder(self) -> CatalogRuntimeBinder {
        self
    }
}

impl<F> IntoCatalogRuntimeBinder for F
where
    F: Fn(&CatalogGeneration) -> Result<CatalogRuntime> + Send + Sync + 'static,
{
    fn into_catalog_runtime_binder(self) -> CatalogRuntimeBinder {
        CatalogRuntimeBinder::new(self)
    }
}

pub(super) fn missing_catalog_runtime_binder(
    generation: &CatalogGeneration,
) -> Result<CatalogRuntime> {
    if generation
        .objects_of_kind(ObjectKind::Table)
        .next()
        .is_none()
        && generation
            .objects_of_kind(ObjectKind::View)
            .next()
            .is_none()
    {
        return CatalogRuntime::new(Vec::new(), Vec::new());
    }
    Err(Error::NotSupported(
        "persistent catalog recovery requires an executor catalog binder".to_owned(),
    ))
}

#[cfg(test)]
#[path = "catalog_runtime_test.rs"]
mod test_binder;

#[cfg(test)]
pub(super) use test_binder::bind_test_catalog_runtime;
