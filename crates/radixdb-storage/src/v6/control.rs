use super::{
    CatalogGeneration, CatalogId, DatabaseGeneration, DatabaseId, FormatError, FormatResult,
    ManifestGeneration, ManifestId, WalGeneration, WalReplayFloor, WriterInstanceId,
};

pub const CONTROL_RECORD_BYTES: usize = 4096;
const CONTROL_MAGIC: [u8; 8] = *b"RDX6CTL\0";
const FORMAT_MAJOR: u16 = 6;
const FORMAT_MINOR: u16 = 0;
const COMMITTED_STATE: u8 = 1;
const CRC_OFFSET: usize = 248;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum ControlSlotIndex {
    Zero = 0,
    One = 1,
}

impl ControlSlotIndex {
    pub fn from_tag(tag: u8) -> FormatResult<Self> {
        match tag {
            0 => Ok(Self::Zero),
            1 => Ok(Self::One),
            _ => Err(FormatError::InvalidControlRecord {
                detail: "slot index is not zero or one",
            }),
        }
    }

    pub const fn tag(self) -> u8 {
        self as u8
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DatabaseManifestRootRef {
    id: ManifestId,
    generation: ManifestGeneration,
    body_sha256: [u8; 32],
}

impl DatabaseManifestRootRef {
    pub const fn new(
        id: ManifestId,
        generation: ManifestGeneration,
        body_sha256: [u8; 32],
    ) -> Self {
        Self {
            id,
            generation,
            body_sha256,
        }
    }

    pub const fn id(self) -> ManifestId {
        self.id
    }

    pub const fn generation(self) -> ManifestGeneration {
        self.generation
    }

    pub const fn body_sha256(&self) -> &[u8; 32] {
        &self.body_sha256
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CatalogRootRef {
    id: CatalogId,
    generation: CatalogGeneration,
    body_sha256: [u8; 32],
}

impl CatalogRootRef {
    pub const fn new(id: CatalogId, generation: CatalogGeneration, body_sha256: [u8; 32]) -> Self {
        Self {
            id,
            generation,
            body_sha256,
        }
    }

    pub const fn id(self) -> CatalogId {
        self.id
    }

    pub const fn generation(self) -> CatalogGeneration {
        self.generation
    }

    pub const fn body_sha256(&self) -> &[u8; 32] {
        &self.body_sha256
    }
}

/// One committed immutable root candidate stored in `CONTROL.0` or
/// `CONTROL.1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ControlRecord {
    slot: ControlSlotIndex,
    database_generation: DatabaseGeneration,
    database_id: DatabaseId,
    database_manifest: DatabaseManifestRootRef,
    catalog: CatalogRootRef,
    wal_replay_floor: WalReplayFloor,
    published_unix_ns: u64,
    writer_instance_id: WriterInstanceId,
}

impl ControlRecord {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        slot: ControlSlotIndex,
        database_generation: DatabaseGeneration,
        database_id: DatabaseId,
        database_manifest: DatabaseManifestRootRef,
        catalog: CatalogRootRef,
        wal_replay_floor: WalReplayFloor,
        published_unix_ns: u64,
        writer_instance_id: WriterInstanceId,
    ) -> FormatResult<Self> {
        if database_manifest.generation().get() != database_generation.get() {
            return Err(FormatError::InvalidControlRecord {
                detail: "database manifest generation differs from database generation",
            });
        }
        Ok(Self {
            slot,
            database_generation,
            database_id,
            database_manifest,
            catalog,
            wal_replay_floor,
            published_unix_ns,
            writer_instance_id,
        })
    }

    pub const fn slot(self) -> ControlSlotIndex {
        self.slot
    }

    pub const fn database_generation(self) -> DatabaseGeneration {
        self.database_generation
    }

    pub const fn database_id(self) -> DatabaseId {
        self.database_id
    }

    pub const fn database_manifest(self) -> DatabaseManifestRootRef {
        self.database_manifest
    }

    pub const fn catalog(self) -> CatalogRootRef {
        self.catalog
    }

    pub const fn wal_replay_floor(self) -> WalReplayFloor {
        self.wal_replay_floor
    }

    pub const fn published_unix_ns(self) -> u64 {
        self.published_unix_ns
    }

    pub const fn writer_instance_id(self) -> WriterInstanceId {
        self.writer_instance_id
    }

    fn same_generation_identity(self, other: Self) -> bool {
        self.database_id == other.database_id
            && self.database_manifest == other.database_manifest
            && self.catalog == other.catalog
            && self.wal_replay_floor == other.wal_replay_floor
    }
}

pub fn encode_control_slot(record: ControlRecord) -> [u8; CONTROL_RECORD_BYTES] {
    let mut output = [0_u8; CONTROL_RECORD_BYTES];
    output[..8].copy_from_slice(&CONTROL_MAGIC);
    put_u16(&mut output, 8, FORMAT_MAJOR);
    put_u16(&mut output, 10, FORMAT_MINOR);
    put_u32(&mut output, 12, CONTROL_RECORD_BYTES as u32);
    output[16] = record.slot().tag();
    output[17] = COMMITTED_STATE;
    put_u64(&mut output, 24, record.database_generation().get());
    output[32..48].copy_from_slice(record.database_id().as_bytes());
    output[48..64].copy_from_slice(record.database_manifest().id().as_bytes());
    put_u64(
        &mut output,
        64,
        record.database_manifest().generation().get(),
    );
    output[72..88].copy_from_slice(record.catalog().id().as_bytes());
    put_u64(&mut output, 88, record.catalog().generation().get());
    put_u64(
        &mut output,
        96,
        record.wal_replay_floor().generation().get(),
    );
    put_u64(&mut output, 104, record.wal_replay_floor().lsn());
    put_u64(&mut output, 112, record.published_unix_ns());
    output[128..160].copy_from_slice(record.database_manifest().body_sha256());
    output[160..192].copy_from_slice(record.catalog().body_sha256());
    output[192..208].copy_from_slice(record.writer_instance_id().as_bytes());
    let crc = control_crc32(&output);
    put_u32(&mut output, CRC_OFFSET, crc);
    output
}

pub fn decode_control_slot(
    bytes: &[u8],
    expected_slot: ControlSlotIndex,
) -> FormatResult<ControlRecord> {
    if bytes.len() != CONTROL_RECORD_BYTES {
        return Err(FormatError::InvalidControlRecord {
            detail: "record length is not exactly 4096 bytes",
        });
    }
    if bytes[..8] != CONTROL_MAGIC {
        return Err(FormatError::InvalidControlRecord {
            detail: "magic does not match RDX6CTL",
        });
    }
    if read_u16(bytes, 8) != FORMAT_MAJOR || read_u16(bytes, 10) != FORMAT_MINOR {
        return Err(FormatError::UnsupportedFormatVersion {
            owner: "CONTROL",
            major: read_u16(bytes, 8),
            minor: read_u16(bytes, 10),
        });
    }
    if read_u32(bytes, 12) != CONTROL_RECORD_BYTES as u32 {
        return Err(FormatError::InvalidControlRecord {
            detail: "declared record length is not 4096",
        });
    }
    let slot = ControlSlotIndex::from_tag(bytes[16])?;
    if slot != expected_slot {
        return Err(FormatError::InvalidControlRecord {
            detail: "slot index differs from filename suffix",
        });
    }
    if bytes[17] != COMMITTED_STATE {
        return Err(FormatError::InvalidControlRecord {
            detail: "slot state is not COMMITTED",
        });
    }
    require_zero(bytes, 18..20, "reserved header bytes are non-zero")?;
    if read_u32(bytes, 20) != 0 {
        return Err(FormatError::InvalidControlRecord {
            detail: "unknown header flags",
        });
    }
    if read_u64(bytes, 120) != 0 {
        return Err(FormatError::InvalidControlRecord {
            detail: "unknown CONTROL flags",
        });
    }
    require_zero(bytes, 208..248, "reserved header bytes are non-zero")?;
    require_zero(
        bytes,
        252..CONTROL_RECORD_BYTES,
        "trailing reserved bytes are non-zero",
    )?;
    if read_u32(bytes, CRC_OFFSET) != control_crc32(bytes) {
        return Err(FormatError::ControlChecksumMismatch);
    }

    let database_generation = DatabaseGeneration::new(read_u64(bytes, 24))?;
    let database_id = DatabaseId::from_bytes(read_array(bytes, 32))?;
    let database_manifest = DatabaseManifestRootRef::new(
        ManifestId::from_bytes(read_array(bytes, 48))?,
        ManifestGeneration::new(read_u64(bytes, 64))?,
        read_array(bytes, 128),
    );
    let catalog = CatalogRootRef::new(
        CatalogId::from_bytes(read_array(bytes, 72))?,
        CatalogGeneration::new(read_u64(bytes, 88))?,
        read_array(bytes, 160),
    );
    let wal_replay_floor = WalReplayFloor::new(
        WalGeneration::new(read_u64(bytes, 96))?,
        read_u64(bytes, 104),
    );
    ControlRecord::new(
        slot,
        database_generation,
        database_id,
        database_manifest,
        catalog,
        wal_replay_floor,
        read_u64(bytes, 112),
        WriterInstanceId::from_bytes(read_array(bytes, 192))?,
    )
}

/// Select from exactly two CONTROL slots. `is_complete` owns full graph
/// reachability validation; a structurally valid but incomplete newer root is
/// skipped in favour of the previous complete generation.
pub fn select_control_slots(
    control_0: &[u8],
    control_1: &[u8],
    mut is_complete: impl FnMut(&ControlRecord) -> bool,
) -> FormatResult<ControlRecord> {
    let first = decode_control_slot(control_0, ControlSlotIndex::Zero).ok();
    let second = decode_control_slot(control_1, ControlSlotIndex::One).ok();
    if first.is_none() && second.is_none() {
        return Err(FormatError::NoValidControlSlot);
    }
    if let (Some(left), Some(right)) = (first, second) {
        if left.database_generation() == right.database_generation()
            && !left.same_generation_identity(right)
        {
            return Err(FormatError::ControlSplitBrain {
                generation: left.database_generation().get(),
            });
        }
    }

    let mut candidates = [first, second].into_iter().flatten().collect::<Vec<_>>();
    candidates.sort_unstable_by(|left, right| {
        right
            .database_generation()
            .cmp(&left.database_generation())
            .then_with(|| left.slot().cmp(&right.slot()))
    });
    candidates
        .into_iter()
        .find(|candidate| is_complete(candidate))
        .ok_or(FormatError::NoCompleteControlGeneration)
}

fn control_crc32(bytes: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&bytes[..CRC_OFFSET]);
    hasher.update(&bytes[CRC_OFFSET + 4..]);
    hasher.finalize()
}

fn require_zero(
    bytes: &[u8],
    range: std::ops::Range<usize>,
    detail: &'static str,
) -> FormatResult<()> {
    if bytes[range].iter().any(|byte| *byte != 0) {
        return Err(FormatError::InvalidControlRecord { detail });
    }
    Ok(())
}

fn read_array<const N: usize>(bytes: &[u8], offset: usize) -> [u8; N] {
    bytes[offset..offset + N]
        .try_into()
        .expect("fixed CONTROL layout was length-checked")
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(read_array(bytes, offset))
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(read_array(bytes, offset))
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(read_array(bytes, offset))
}

fn put_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}
