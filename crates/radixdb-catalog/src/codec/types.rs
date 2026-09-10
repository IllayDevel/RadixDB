use crate::{CatalogError, CatalogGraph, CatalogResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogPackMeta {
    database_id: [u8; 16],
    catalog_id: [u8; 16],
    catalog_generation: u64,
    snapshot_lsn: u64,
    created_unix_ns: u64,
}

impl CatalogPackMeta {
    pub fn new(
        database_id: [u8; 16],
        catalog_id: [u8; 16],
        catalog_generation: u64,
        snapshot_lsn: u64,
        created_unix_ns: u64,
    ) -> CatalogResult<Self> {
        if database_id == [0; 16] {
            return Err(CatalogError::InvalidCatalogFormat {
                detail: "database_id is zero",
            });
        }
        if catalog_id == [0; 16] {
            return Err(CatalogError::InvalidCatalogFormat {
                detail: "catalog_id is zero",
            });
        }
        if catalog_generation == 0 {
            return Err(CatalogError::InvalidCatalogFormat {
                detail: "catalog generation is zero",
            });
        }
        Ok(Self {
            database_id,
            catalog_id,
            catalog_generation,
            snapshot_lsn,
            created_unix_ns,
        })
    }

    pub const fn database_id(self) -> [u8; 16] {
        self.database_id
    }

    pub const fn catalog_id(self) -> [u8; 16] {
        self.catalog_id
    }

    pub const fn catalog_generation(self) -> u64 {
        self.catalog_generation
    }

    pub const fn snapshot_lsn(self) -> u64 {
        self.snapshot_lsn
    }

    pub const fn created_unix_ns(self) -> u64 {
        self.created_unix_ns
    }
}

#[derive(Debug, Clone)]
pub struct CatalogPack {
    format_minor: u16,
    meta: CatalogPackMeta,
    graph: CatalogGraph,
    body_sha256: [u8; 32],
}

impl CatalogPack {
    pub(crate) const fn new(
        format_minor: u16,
        meta: CatalogPackMeta,
        graph: CatalogGraph,
        body_sha256: [u8; 32],
    ) -> Self {
        Self {
            format_minor,
            meta,
            graph,
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

    pub const fn body_sha256(&self) -> &[u8; 32] {
        &self.body_sha256
    }

    pub fn into_parts(self) -> (u16, CatalogPackMeta, CatalogGraph, [u8; 32]) {
        (self.format_minor, self.meta, self.graph, self.body_sha256)
    }
}
