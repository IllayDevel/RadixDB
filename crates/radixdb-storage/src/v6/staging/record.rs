use super::super::{
    DatabaseGeneration, FormatError, FormatResult, PublicationId, WriterInstanceId,
};

pub const STAGING_OWNER_BYTES: usize = 128;
pub const STAGING_COMPLETE_BYTES: usize = 128;

const OWNER_MAGIC: &[u8; 8] = b"RDX6STG\0"; // versioned-format-name
const COMPLETE_MAGIC: &[u8; 8] = b"RDX6CMP\0"; // versioned-format-name
const FORMAT_MAJOR: u16 = 6;
const FORMAT_MINOR: u16 = 0;
const CRC_OFFSET: usize = 124;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StagingOwner {
    writer_instance_id: WriterInstanceId,
    publication_id: PublicationId,
    process_id: u64,
    created_unix_ns: u64,
    heartbeat_unix_ns: u64,
    intended_generation: DatabaseGeneration,
}

impl StagingOwner {
    pub fn new(
        writer_instance_id: WriterInstanceId,
        publication_id: PublicationId,
        process_id: u64,
        created_unix_ns: u64,
        heartbeat_unix_ns: u64,
        intended_generation: DatabaseGeneration,
    ) -> FormatResult<Self> {
        if created_unix_ns == 0 {
            return invalid("OWNER", "creation timestamp is zero");
        }
        if heartbeat_unix_ns < created_unix_ns {
            return invalid("OWNER", "heartbeat predates creation");
        }
        Ok(Self {
            writer_instance_id,
            publication_id,
            process_id,
            created_unix_ns,
            heartbeat_unix_ns,
            intended_generation,
        })
    }

    pub const fn writer_instance_id(self) -> WriterInstanceId {
        self.writer_instance_id
    }

    pub const fn publication_id(self) -> PublicationId {
        self.publication_id
    }

    pub const fn process_id(self) -> u64 {
        self.process_id
    }

    pub const fn created_unix_ns(self) -> u64 {
        self.created_unix_ns
    }

    pub const fn heartbeat_unix_ns(self) -> u64 {
        self.heartbeat_unix_ns
    }

    pub const fn intended_generation(self) -> DatabaseGeneration {
        self.intended_generation
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StagingComplete {
    writer_instance_id: WriterInstanceId,
    publication_id: PublicationId,
    intended_generation: DatabaseGeneration,
    member_count: u64,
    members_sha256: [u8; 32],
    completed_unix_ns: u64,
}

impl StagingComplete {
    pub fn new(
        owner: StagingOwner,
        member_count: u64,
        members_sha256: [u8; 32],
        completed_unix_ns: u64,
    ) -> FormatResult<Self> {
        if completed_unix_ns < owner.created_unix_ns() {
            return invalid("COMPLETE", "completion predates creation");
        }
        Ok(Self {
            writer_instance_id: owner.writer_instance_id(),
            publication_id: owner.publication_id(),
            intended_generation: owner.intended_generation(),
            member_count,
            members_sha256,
            completed_unix_ns,
        })
    }

    pub const fn writer_instance_id(self) -> WriterInstanceId {
        self.writer_instance_id
    }

    pub const fn publication_id(self) -> PublicationId {
        self.publication_id
    }

    pub const fn intended_generation(self) -> DatabaseGeneration {
        self.intended_generation
    }

    pub const fn member_count(self) -> u64 {
        self.member_count
    }

    pub const fn members_sha256(self) -> [u8; 32] {
        self.members_sha256
    }

    pub const fn completed_unix_ns(self) -> u64 {
        self.completed_unix_ns
    }
}

pub fn encode_staging_owner(owner: StagingOwner) -> [u8; STAGING_OWNER_BYTES] {
    let mut output = [0_u8; STAGING_OWNER_BYTES];
    output[..8].copy_from_slice(OWNER_MAGIC);
    put_common_header(&mut output);
    output[16..32].copy_from_slice(owner.writer_instance_id().as_bytes());
    output[32..48].copy_from_slice(owner.publication_id().as_bytes());
    output[48..56].copy_from_slice(&owner.process_id().to_le_bytes());
    output[56..64].copy_from_slice(&owner.created_unix_ns().to_le_bytes());
    output[64..72].copy_from_slice(&owner.heartbeat_unix_ns().to_le_bytes());
    output[72..80].copy_from_slice(&owner.intended_generation().get().to_le_bytes());
    finish_crc(&mut output);
    output
}

pub fn decode_staging_owner(bytes: &[u8]) -> FormatResult<StagingOwner> {
    validate_record(bytes, OWNER_MAGIC, "OWNER")?;
    if bytes[80..124].iter().any(|byte| *byte != 0) {
        return invalid("OWNER", "flags or reserved bytes are non-zero");
    }
    StagingOwner::new(
        WriterInstanceId::from_bytes(read_array(bytes, 16))?,
        PublicationId::from_bytes(read_array(bytes, 32))?,
        read_u64(bytes, 48),
        read_u64(bytes, 56),
        read_u64(bytes, 64),
        DatabaseGeneration::new(read_u64(bytes, 72))?,
    )
}

pub fn encode_staging_complete(complete: StagingComplete) -> [u8; STAGING_COMPLETE_BYTES] {
    let mut output = [0_u8; STAGING_COMPLETE_BYTES];
    output[..8].copy_from_slice(COMPLETE_MAGIC);
    put_common_header(&mut output);
    output[16..32].copy_from_slice(complete.writer_instance_id().as_bytes());
    output[32..48].copy_from_slice(complete.publication_id().as_bytes());
    output[48..56].copy_from_slice(&complete.intended_generation().get().to_le_bytes());
    output[56..64].copy_from_slice(&complete.member_count().to_le_bytes());
    output[64..96].copy_from_slice(&complete.members_sha256());
    output[96..104].copy_from_slice(&complete.completed_unix_ns().to_le_bytes());
    finish_crc(&mut output);
    output
}

pub fn decode_staging_complete(bytes: &[u8]) -> FormatResult<StagingComplete> {
    validate_record(bytes, COMPLETE_MAGIC, "COMPLETE")?;
    if bytes[104..124].iter().any(|byte| *byte != 0) {
        return invalid("COMPLETE", "flags or reserved bytes are non-zero");
    }
    let owner = StagingOwner::new(
        WriterInstanceId::from_bytes(read_array(bytes, 16))?,
        PublicationId::from_bytes(read_array(bytes, 32))?,
        0,
        1,
        1,
        DatabaseGeneration::new(read_u64(bytes, 48))?,
    )?;
    StagingComplete::new(
        owner,
        read_u64(bytes, 56),
        read_array(bytes, 64),
        read_u64(bytes, 96),
    )
}

fn put_common_header(output: &mut [u8; 128]) {
    output[8..10].copy_from_slice(&FORMAT_MAJOR.to_le_bytes());
    output[10..12].copy_from_slice(&FORMAT_MINOR.to_le_bytes());
    output[12..16].copy_from_slice(&(STAGING_OWNER_BYTES as u32).to_le_bytes());
}

fn finish_crc(output: &mut [u8; 128]) {
    let crc = radixdb_core::crc32_ieee(&output[..CRC_OFFSET]);
    output[CRC_OFFSET..].copy_from_slice(&crc.to_le_bytes());
}

fn validate_record(bytes: &[u8], magic: &[u8; 8], record: &'static str) -> FormatResult<()> {
    if bytes.len() != STAGING_OWNER_BYTES {
        return invalid(record, "record length is not 128 bytes");
    }
    if &bytes[..8] != magic {
        return invalid(record, "magic mismatch");
    }
    let major = u16::from_le_bytes([bytes[8], bytes[9]]);
    let minor = u16::from_le_bytes([bytes[10], bytes[11]]);
    if major != FORMAT_MAJOR || minor != FORMAT_MINOR {
        return Err(FormatError::UnsupportedFormatVersion {
            owner: record,
            major,
            minor,
        });
    }
    if u32::from_le_bytes(bytes[12..16].try_into().expect("fixed slice")) as usize
        != STAGING_OWNER_BYTES
    {
        return invalid(record, "declared record length mismatch");
    }
    if read_u32(bytes, CRC_OFFSET) != radixdb_core::crc32_ieee(&bytes[..CRC_OFFSET]) {
        return Err(FormatError::StagingChecksumMismatch { record });
    }
    Ok(())
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("fixed slice"))
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("fixed slice"))
}

fn read_array<const N: usize>(bytes: &[u8], offset: usize) -> [u8; N] {
    bytes[offset..offset + N].try_into().expect("fixed slice")
}

fn invalid<T>(record: &'static str, detail: &'static str) -> FormatResult<T> {
    Err(FormatError::InvalidStagingRecord { record, detail })
}
