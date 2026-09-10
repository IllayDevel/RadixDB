use std::path::PathBuf;

use super::super::{
    ArtifactKind, ArtifactRef, CatalogRef, CatalogRootRef, DatabaseGeneration, DatabaseId,
    DatabaseManifestRootRef, FormatError, FormatResult, PhysicalGenerationSnapshot, SnapshotId,
    TableManifestRef, WalGeneration, ARTIFACT_CODEC_VERSION, FORMAT_VERSION,
    MAX_ARTIFACT_FILE_BYTES, MAX_CATALOG_FILE_BYTES, MAX_MANIFEST_FILE_BYTES,
};

pub const MAX_SNAPSHOT_MEMBERS: usize = 8_388_608;
pub const MAX_SNAPSHOT_MANIFEST_BYTES: usize =
    256 + MAX_SNAPSHOT_MEMBERS * SNAPSHOT_MEMBER_BYTES + 48;
pub const SNAPSHOT_MEMBER_BYTES: usize = 96;
pub const MAX_SNAPSHOT_WAL_BYTES: u64 = MAX_ARTIFACT_FILE_BYTES;

const OPTIONAL_REBUILDABLE: u32 = 1;
const MIN_COMMON_FILE_BYTES: u64 = 256 + 48;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotIndexPolicy {
    Include,
    OmitRebuildable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u16)]
pub enum SnapshotMemberKind {
    DatabaseManifest = 1,
    Catalog = 2,
    TableManifest = 3,
    Data = 4,
    Index = 5,
    Wal = 6,
}

impl SnapshotMemberKind {
    pub fn from_tag(tag: u16) -> FormatResult<Self> {
        match tag {
            1 => Ok(Self::DatabaseManifest),
            2 => Ok(Self::Catalog),
            3 => Ok(Self::TableManifest),
            4 => Ok(Self::Data),
            5 => Ok(Self::Index),
            6 => Ok(Self::Wal),
            _ => invalid("unknown member kind"),
        }
    }

    pub const fn tag(self) -> u16 {
        self as u16
    }

    pub const fn suffix(self) -> SnapshotLocatorSuffix {
        match self {
            Self::DatabaseManifest | Self::TableManifest => SnapshotLocatorSuffix::Manifest,
            Self::Catalog => SnapshotLocatorSuffix::Catalog,
            Self::Data => SnapshotLocatorSuffix::Data,
            Self::Index => SnapshotLocatorSuffix::Index,
            Self::Wal => SnapshotLocatorSuffix::Wal,
        }
    }

    const fn directory(self) -> &'static str {
        match self {
            Self::DatabaseManifest => "database-manifest",
            Self::Catalog => "catalog",
            Self::TableManifest => "table-manifest",
            Self::Data => "data",
            Self::Index => "index",
            Self::Wal => "wal",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum SnapshotLocatorSuffix {
    Data = 1,
    Index = 2,
    Manifest = 3,
    Catalog = 4,
    Wal = 5,
}

impl SnapshotLocatorSuffix {
    pub fn from_tag(tag: u16) -> FormatResult<Self> {
        match tag {
            1 => Ok(Self::Data),
            2 => Ok(Self::Index),
            3 => Ok(Self::Manifest),
            4 => Ok(Self::Catalog),
            5 => Ok(Self::Wal),
            _ => invalid("unknown member locator suffix"),
        }
    }

    pub const fn tag(self) -> u16 {
        self as u16
    }

    const fn extension(self) -> &'static str {
        match self {
            Self::Data => "data",
            Self::Index => "idx",
            Self::Manifest => "mft",
            Self::Catalog => "cat",
            Self::Wal => "log",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SnapshotMember {
    kind: SnapshotMemberKind,
    format_version: u16,
    optional_rebuildable: bool,
    id: [u8; 16],
    generation: u64,
    byte_length: u64,
    body_sha256: [u8; 32],
    locator_shard: u16,
    locator_suffix: SnapshotLocatorSuffix,
}

impl SnapshotMember {
    #[allow(clippy::too_many_arguments)]
    pub fn from_persisted(
        kind: SnapshotMemberKind,
        format_version: u16,
        flags: u32,
        id: [u8; 16],
        generation: u64,
        byte_length: u64,
        body_sha256: [u8; 32],
        locator_shard: u16,
        locator_suffix: u16,
    ) -> FormatResult<Self> {
        let expected_version = match kind {
            SnapshotMemberKind::Data | SnapshotMemberKind::Index => ARTIFACT_CODEC_VERSION,
            SnapshotMemberKind::DatabaseManifest
            | SnapshotMemberKind::Catalog
            | SnapshotMemberKind::TableManifest
            | SnapshotMemberKind::Wal => FORMAT_VERSION.major(),
        };
        if format_version != expected_version {
            return invalid("member format version is unsupported for its kind");
        }
        if id == [0; 16] {
            return invalid("member identity is zero");
        }
        if generation == 0 {
            return invalid("member generation is zero");
        }
        let (minimum_bytes, maximum_bytes) = match kind {
            SnapshotMemberKind::DatabaseManifest | SnapshotMemberKind::TableManifest => {
                (MIN_COMMON_FILE_BYTES, MAX_MANIFEST_FILE_BYTES)
            }
            SnapshotMemberKind::Catalog => (MIN_COMMON_FILE_BYTES, MAX_CATALOG_FILE_BYTES),
            SnapshotMemberKind::Data | SnapshotMemberKind::Index => {
                (MIN_COMMON_FILE_BYTES, MAX_ARTIFACT_FILE_BYTES)
            }
            // A freshly published database owns an empty first WAL
            // generation. It is still a required typed member: zero bytes are
            // a valid exact snapshot of that immutable generation.
            SnapshotMemberKind::Wal => (0, MAX_SNAPSHOT_WAL_BYTES),
        };
        if byte_length < minimum_bytes || byte_length > maximum_bytes {
            return Err(FormatError::SnapshotLimitExceeded {
                field: "member bytes",
                actual: byte_length,
                limit: maximum_bytes,
            });
        }
        if flags & !OPTIONAL_REBUILDABLE != 0 {
            return invalid("unknown member flags");
        }
        let optional_rebuildable = flags & OPTIONAL_REBUILDABLE != 0;
        if optional_rebuildable && kind != SnapshotMemberKind::Index {
            return invalid("OPTIONAL_REBUILDABLE belongs only to INDEX");
        }
        let expected_shard = match kind {
            SnapshotMemberKind::Data | SnapshotMemberKind::Index => u16::from(id[0]),
            SnapshotMemberKind::DatabaseManifest
            | SnapshotMemberKind::Catalog
            | SnapshotMemberKind::TableManifest
            | SnapshotMemberKind::Wal => 0,
        };
        if locator_shard != expected_shard {
            return invalid("member locator shard is not canonical");
        }
        let locator_suffix = SnapshotLocatorSuffix::from_tag(locator_suffix)?;
        if locator_suffix != kind.suffix() {
            return invalid("member locator suffix differs from member kind");
        }
        Ok(Self {
            kind,
            format_version,
            optional_rebuildable,
            id,
            generation,
            byte_length,
            body_sha256,
            locator_shard,
            locator_suffix,
        })
    }

    pub fn database_manifest(
        reference: DatabaseManifestRootRef,
        byte_length: u64,
    ) -> FormatResult<Self> {
        Self::new(
            SnapshotMemberKind::DatabaseManifest,
            FORMAT_VERSION.major(),
            false,
            reference.id().into_bytes(),
            reference.generation().get(),
            byte_length,
            *reference.body_sha256(),
        )
    }

    pub fn catalog(reference: CatalogRef) -> FormatResult<Self> {
        Self::new(
            SnapshotMemberKind::Catalog,
            reference.format().major(),
            false,
            reference.id().into_bytes(),
            reference.generation().get(),
            reference.byte_length(),
            *reference.body_sha256(),
        )
    }

    pub fn table_manifest(reference: TableManifestRef) -> FormatResult<Self> {
        let reference = reference.manifest();
        Self::new(
            SnapshotMemberKind::TableManifest,
            reference.format().major(),
            false,
            reference.id().into_bytes(),
            reference.generation().get(),
            reference.byte_length(),
            *reference.body_sha256(),
        )
    }

    pub fn artifact(reference: ArtifactRef) -> FormatResult<Self> {
        let kind = match reference.kind() {
            ArtifactKind::Data => SnapshotMemberKind::Data,
            ArtifactKind::Index => SnapshotMemberKind::Index,
        };
        Self::new(
            kind,
            ARTIFACT_CODEC_VERSION,
            kind == SnapshotMemberKind::Index,
            reference.id().into_bytes(),
            reference.creation_generation().get(),
            reference.byte_length(),
            *reference.body_sha256(),
        )
    }

    pub fn wal(
        database_id: DatabaseId,
        generation: WalGeneration,
        byte_length: u64,
        body_sha256: [u8; 32],
    ) -> FormatResult<Self> {
        Self::new(
            SnapshotMemberKind::Wal,
            FORMAT_VERSION.major(),
            false,
            wal_member_id(database_id, generation),
            generation.get(),
            byte_length,
            body_sha256,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new(
        kind: SnapshotMemberKind,
        format_version: u16,
        optional_rebuildable: bool,
        id: [u8; 16],
        generation: u64,
        byte_length: u64,
        body_sha256: [u8; 32],
    ) -> FormatResult<Self> {
        let flags = u32::from(optional_rebuildable) * OPTIONAL_REBUILDABLE;
        let shard = match kind {
            SnapshotMemberKind::Data | SnapshotMemberKind::Index => u16::from(id[0]),
            _ => 0,
        };
        Self::from_persisted(
            kind,
            format_version,
            flags,
            id,
            generation,
            byte_length,
            body_sha256,
            shard,
            kind.suffix().tag(),
        )
    }

    pub const fn kind(self) -> SnapshotMemberKind {
        self.kind
    }

    pub const fn format_version(self) -> u16 {
        self.format_version
    }

    pub const fn flags(self) -> u32 {
        if self.optional_rebuildable {
            OPTIONAL_REBUILDABLE
        } else {
            0
        }
    }

    pub const fn optional_rebuildable(self) -> bool {
        self.optional_rebuildable
    }

    pub const fn id(self) -> [u8; 16] {
        self.id
    }

    pub const fn generation(self) -> u64 {
        self.generation
    }

    pub const fn byte_length(self) -> u64 {
        self.byte_length
    }

    pub const fn body_sha256(self) -> [u8; 32] {
        self.body_sha256
    }

    pub const fn locator_shard(self) -> u16 {
        self.locator_shard
    }

    pub const fn locator_suffix(self) -> SnapshotLocatorSuffix {
        self.locator_suffix
    }

    pub fn relative_path(self) -> PathBuf {
        let mut path = PathBuf::from("members").join(self.kind.directory());
        if matches!(
            self.kind,
            SnapshotMemberKind::Data | SnapshotMemberKind::Index
        ) {
            path.push(format!("{:02x}", self.locator_shard));
        }
        path.join(format!(
            "{}.{}",
            identity_hex(self.id),
            self.locator_suffix.extension()
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotManifest {
    snapshot_id: SnapshotId,
    database_id: DatabaseId,
    database_generation: DatabaseGeneration,
    database_manifest: DatabaseManifestRootRef,
    catalog: CatalogRootRef,
    members: Vec<SnapshotMember>,
    created_unix_ns: u64,
}

impl SnapshotManifest {
    /// Build the exact member set from one pinned immutable generation and a
    /// typed contiguous WAL range supplied by the WAL owner.
    #[allow(clippy::too_many_arguments)]
    pub fn from_generation(
        snapshot_id: SnapshotId,
        generation: &PhysicalGenerationSnapshot,
        database_manifest_byte_length: u64,
        mut wal_members: Vec<SnapshotMember>,
        index_policy: SnapshotIndexPolicy,
        created_unix_ns: u64,
    ) -> FormatResult<Self> {
        if wal_members.is_empty()
            || wal_members
                .iter()
                .any(|member| member.kind() != SnapshotMemberKind::Wal)
        {
            return invalid("snapshot requires a non-empty typed WAL member range");
        }
        wal_members.sort_unstable_by_key(|member| member.generation());
        let wal_floor = generation
            .database_manifest()
            .wal_replay_floor()
            .generation()
            .get();
        if wal_members[0].generation() != wal_floor
            || wal_members.windows(2).any(|pair| {
                pair[0]
                    .generation()
                    .checked_add(1)
                    .is_none_or(|expected| pair[1].generation() != expected)
            })
        {
            return invalid("snapshot WAL members are not contiguous from the replay floor");
        }

        let control = generation.control();
        let mut members = Vec::new();
        members.push(SnapshotMember::database_manifest(
            control.database_manifest(),
            database_manifest_byte_length,
        )?);
        members.push(SnapshotMember::catalog(
            generation.database_manifest().catalog(),
        )?);
        for reference in generation.database_manifest().tables() {
            members.push(SnapshotMember::table_manifest(*reference)?);
        }
        for table in generation.table_manifests() {
            for segment in table.segments() {
                members.push(SnapshotMember::artifact(segment.data_artifact())?);
                if index_policy == SnapshotIndexPolicy::Include {
                    if let Some(index) = segment.index_artifact() {
                        members.push(SnapshotMember::artifact(index)?);
                    }
                }
            }
        }
        members.append(&mut wal_members);
        Self::new(
            snapshot_id,
            control.database_id(),
            control.database_generation(),
            control.database_manifest(),
            control.catalog(),
            members,
            created_unix_ns,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        snapshot_id: SnapshotId,
        database_id: DatabaseId,
        database_generation: DatabaseGeneration,
        database_manifest: DatabaseManifestRootRef,
        catalog: CatalogRootRef,
        mut members: Vec<SnapshotMember>,
        created_unix_ns: u64,
    ) -> FormatResult<Self> {
        if created_unix_ns == 0 {
            return invalid("creation timestamp is zero");
        }
        if database_manifest.generation().get() != database_generation.get() {
            return invalid("database-manifest generation differs from snapshot generation");
        }
        if members.len() > MAX_SNAPSHOT_MEMBERS {
            return Err(FormatError::SnapshotLimitExceeded {
                field: "member count",
                actual: members.len() as u64,
                limit: MAX_SNAPSHOT_MEMBERS as u64,
            });
        }
        members.sort_unstable_by_key(|member| (member.kind(), member.id()));
        if members
            .windows(2)
            .any(|pair| (pair[0].kind(), pair[0].id()) == (pair[1].kind(), pair[1].id()))
        {
            return invalid("duplicate member identity");
        }

        let database_members = members
            .iter()
            .filter(|member| member.kind() == SnapshotMemberKind::DatabaseManifest)
            .collect::<Vec<_>>();
        if database_members.len() != 1 {
            return invalid("snapshot must contain exactly one database manifest");
        }
        let database_member = database_members[0];
        if database_member.id() != database_manifest.id().into_bytes()
            || database_member.generation() != database_manifest.generation().get()
            || database_member.body_sha256() != *database_manifest.body_sha256()
        {
            return invalid("database-manifest member differs from snapshot root");
        }

        let catalog_members = members
            .iter()
            .filter(|member| member.kind() == SnapshotMemberKind::Catalog)
            .collect::<Vec<_>>();
        if catalog_members.len() != 1 {
            return invalid("snapshot must contain exactly one catalog");
        }
        let catalog_member = catalog_members[0];
        if catalog_member.id() != catalog.id().into_bytes()
            || catalog_member.generation() != catalog.generation().get()
            || catalog_member.body_sha256() != *catalog.body_sha256()
        {
            return invalid("catalog member differs from snapshot root");
        }

        Ok(Self {
            snapshot_id,
            database_id,
            database_generation,
            database_manifest,
            catalog,
            members,
            created_unix_ns,
        })
    }

    pub const fn snapshot_id(&self) -> SnapshotId {
        self.snapshot_id
    }

    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    pub const fn database_generation(&self) -> DatabaseGeneration {
        self.database_generation
    }

    pub const fn database_manifest(&self) -> DatabaseManifestRootRef {
        self.database_manifest
    }

    pub const fn catalog(&self) -> CatalogRootRef {
        self.catalog
    }

    pub fn members(&self) -> &[SnapshotMember] {
        &self.members
    }

    pub const fn created_unix_ns(&self) -> u64 {
        self.created_unix_ns
    }
}

fn wal_member_id(database_id: DatabaseId, generation: WalGeneration) -> [u8; 16] {
    let mut identity = Vec::with_capacity(48);
    identity.extend_from_slice(b"radixdb-wal-member-id\0");
    identity.extend_from_slice(database_id.as_bytes());
    identity.extend_from_slice(&generation.get().to_le_bytes());
    let digest = radixdb_core::sha256_digest(&identity);
    digest[..16].try_into().expect("fixed SHA-256 prefix")
}

fn identity_hex(identity: [u8; 16]) -> String {
    let mut encoded = String::with_capacity(32);
    for byte in identity {
        use std::fmt::Write;
        write!(&mut encoded, "{byte:02x}").expect("writing into String cannot fail");
    }
    encoded
}

fn invalid<T>(detail: &'static str) -> FormatResult<T> {
    Err(FormatError::InvalidSnapshot { detail })
}
