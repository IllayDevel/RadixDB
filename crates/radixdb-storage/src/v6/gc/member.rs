use std::path::PathBuf;

use radixdb_catalog::ObjectId;

use crate::v6::{
    catalog_path, database_manifest_path, table_manifest_path, ArtifactId, ArtifactRef,
    CatalogGeneration, CatalogRef, DatabaseManifestRootRef, FormatResult, ManifestGeneration,
    ManifestKind, ManifestRef, TableManifestRef,
};

/// Exact immutable member of one published physical generation.
///
/// The enum is deliberately private to the lifecycle owner: callers continue
/// to use typed catalog/manifest/artifact references, while cleanup reasons
/// about their complete canonical locator and exact persisted identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ImmutableMemberRef {
    DatabaseManifest(ManifestRef),
    Catalog(CatalogRef),
    TableManifest(TableManifestRef),
    Artifact(ArtifactRef),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ImmutableMemberLocator {
    DatabaseManifest(ManifestGeneration),
    Catalog(CatalogGeneration),
    TableManifest(ObjectId, ManifestGeneration),
    Artifact(ArtifactId),
}

impl ImmutableMemberRef {
    pub(crate) fn database_manifest(
        reference: DatabaseManifestRootRef,
        byte_length: u64,
    ) -> FormatResult<Self> {
        Ok(Self::DatabaseManifest(ManifestRef::new(
            reference.id(),
            ManifestKind::Database,
            reference.generation(),
            byte_length,
            *reference.body_sha256(),
        )?))
    }

    pub(crate) const fn locator(self) -> ImmutableMemberLocator {
        match self {
            Self::DatabaseManifest(reference) => {
                ImmutableMemberLocator::DatabaseManifest(reference.generation())
            }
            Self::Catalog(reference) => ImmutableMemberLocator::Catalog(reference.generation()),
            Self::TableManifest(reference) => ImmutableMemberLocator::TableManifest(
                reference.table_id(),
                reference.manifest().generation(),
            ),
            Self::Artifact(reference) => ImmutableMemberLocator::Artifact(reference.id()),
        }
    }

    pub(crate) fn relative_path(self) -> PathBuf {
        match self {
            Self::DatabaseManifest(reference) => {
                database_manifest_path(DatabaseManifestRootRef::new(
                    reference.id(),
                    reference.generation(),
                    *reference.body_sha256(),
                ))
            }
            Self::Catalog(reference) => catalog_path(reference),
            Self::TableManifest(reference) => table_manifest_path(reference),
            Self::Artifact(reference) => reference.relative_path(),
        }
    }

    pub(crate) const fn byte_length(self) -> u64 {
        match self {
            Self::DatabaseManifest(reference) => reference.byte_length(),
            Self::Catalog(reference) => reference.byte_length(),
            Self::TableManifest(reference) => reference.manifest().byte_length(),
            Self::Artifact(reference) => reference.byte_length(),
        }
    }
}
