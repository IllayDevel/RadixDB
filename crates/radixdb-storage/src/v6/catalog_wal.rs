use std::fs::File;
use std::io::{Seek, SeekFrom, Write};

use radixdb_catalog::{
    decode_catalog_mutation_set, encode_catalog_mutation_set, CatalogGeneration,
    CatalogMutationSet, CatalogPackMeta, MAX_CATALOG_MUTATION_SET_BYTES,
};

use super::{fault::reach_generation_boundary, FormatError, FormatResult, GenerationCrashPoint};

pub const CATALOG_WAL_FORMAT_MAJOR: u16 = 6;
pub const CATALOG_WAL_FORMAT_MINOR: u16 = 0;
pub const CATALOG_WAL_RECORD_HEADER_BYTES: usize = 160;
pub const MAX_CATALOG_WAL_RECORD_BYTES: u64 =
    MAX_CATALOG_MUTATION_SET_BYTES + CATALOG_WAL_RECORD_HEADER_BYTES as u64;
pub const MAX_CATALOG_WAL_REPLAY_BYTES: u64 = 1024 * 1024 * 1024;
pub const MAX_CATALOG_WAL_REPLAY_TRANSACTIONS: u64 = 262_144;

const MAGIC: &[u8; 8] = b"RDX6CWAL";
const HEADER_CRC_OFFSET: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
enum RecordKind {
    Mutation = 1,
    Commit = 2,
}

impl TryFrom<u16> for RecordKind {
    type Error = FormatError;

    fn try_from(value: u16) -> FormatResult<Self> {
        match value {
            1 => Ok(Self::Mutation),
            2 => Ok(Self::Commit),
            _ => invalid("record kind is unknown"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CatalogWalTransactionId([u8; 16]);

impl CatalogWalTransactionId {
    pub fn from_bytes(bytes: [u8; 16]) -> FormatResult<Self> {
        if bytes == [0; 16] {
            return invalid("transaction identity is zero");
        }
        Ok(Self(bytes))
    }

    pub const fn as_bytes(self) -> [u8; 16] {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogWalTransaction {
    transaction_id: CatalogWalTransactionId,
    successor_catalog_id: [u8; 16],
    commit_lsn: u64,
    created_unix_ns: u64,
    mutation_set: CatalogMutationSet,
}

impl CatalogWalTransaction {
    pub fn new(
        transaction_id: CatalogWalTransactionId,
        successor_catalog_id: [u8; 16],
        commit_lsn: u64,
        created_unix_ns: u64,
        mutation_set: CatalogMutationSet,
    ) -> FormatResult<Self> {
        if successor_catalog_id == [0; 16] {
            return invalid("successor catalog identity is zero");
        }
        if &successor_catalog_id == mutation_set.expected_catalog_id() {
            return invalid("successor catalog identity equals source identity");
        }
        if commit_lsn == 0 {
            return invalid("commit LSN is zero");
        }
        mutation_set
            .expected_catalog_generation()
            .checked_add(1)
            .ok_or(FormatError::InvalidCatalogWal {
                detail: "successor catalog generation overflows",
            })?;
        Ok(Self {
            transaction_id,
            successor_catalog_id,
            commit_lsn,
            created_unix_ns,
            mutation_set,
        })
    }

    pub const fn transaction_id(&self) -> CatalogWalTransactionId {
        self.transaction_id
    }

    pub const fn successor_catalog_id(&self) -> [u8; 16] {
        self.successor_catalog_id
    }

    pub const fn commit_lsn(&self) -> u64 {
        self.commit_lsn
    }

    pub const fn created_unix_ns(&self) -> u64 {
        self.created_unix_ns
    }

    pub const fn mutation_set(&self) -> &CatalogMutationSet {
        &self.mutation_set
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogWalReplayLimits {
    max_stream_bytes: u64,
    max_transactions: u64,
    max_record_bytes: u64,
}

impl CatalogWalReplayLimits {
    pub fn new(
        max_stream_bytes: u64,
        max_transactions: u64,
        max_record_bytes: u64,
    ) -> FormatResult<Self> {
        require_runtime_limit(
            "stream bytes",
            max_stream_bytes,
            MAX_CATALOG_WAL_REPLAY_BYTES,
        )?;
        require_runtime_limit(
            "transaction count",
            max_transactions,
            MAX_CATALOG_WAL_REPLAY_TRANSACTIONS,
        )?;
        require_runtime_limit(
            "record bytes",
            max_record_bytes,
            MAX_CATALOG_WAL_RECORD_BYTES,
        )?;
        if max_stream_bytes < CATALOG_WAL_RECORD_HEADER_BYTES as u64
            || max_transactions == 0
            || max_record_bytes < CATALOG_WAL_RECORD_HEADER_BYTES as u64
        {
            return invalid("runtime replay limit is below the minimum envelope");
        }
        Ok(Self {
            max_stream_bytes,
            max_transactions,
            max_record_bytes,
        })
    }

    pub const fn hard() -> Self {
        Self {
            max_stream_bytes: MAX_CATALOG_WAL_REPLAY_BYTES,
            max_transactions: MAX_CATALOG_WAL_REPLAY_TRANSACTIONS,
            max_record_bytes: MAX_CATALOG_WAL_RECORD_BYTES,
        }
    }

    pub const fn max_stream_bytes(self) -> u64 {
        self.max_stream_bytes
    }

    pub const fn max_transactions(self) -> u64 {
        self.max_transactions
    }

    pub const fn max_record_bytes(self) -> u64 {
        self.max_record_bytes
    }
}

impl Default for CatalogWalReplayLimits {
    fn default() -> Self {
        Self::hard()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogWalReplay {
    transactions: Vec<CatalogWalTransaction>,
    committed_bytes: usize,
    incomplete_tail_bytes: usize,
}

impl CatalogWalReplay {
    pub fn transactions(&self) -> &[CatalogWalTransaction] {
        &self.transactions
    }

    pub const fn committed_bytes(&self) -> usize {
        self.committed_bytes
    }

    pub const fn incomplete_tail_bytes(&self) -> usize {
        self.incomplete_tail_bytes
    }
}

struct Record<'a> {
    kind: RecordKind,
    transaction_id: CatalogWalTransactionId,
    sequence: u32,
    expected_generation: u64,
    payload_sha256: [u8; 32],
    successor_catalog_id: [u8; 16],
    commit_lsn: u64,
    created_unix_ns: u64,
    record_count: u32,
    payload: &'a [u8],
    length: usize,
}

enum ReadRecord<'a> {
    Complete(Record<'a>),
    Incomplete,
}

pub fn encode_catalog_wal_transaction(
    transaction: &CatalogWalTransaction,
) -> FormatResult<Vec<u8>> {
    let payload = encode_catalog_mutation_set(transaction.mutation_set())?;
    let payload_sha256 = radixdb_core::sha256_digest(&payload);
    let mutation = encode_record(
        transaction,
        RecordKind::Mutation,
        0,
        0,
        &payload,
        payload_sha256,
    )?;
    let commit = encode_record(transaction, RecordKind::Commit, 1, 1, &[], payload_sha256)?;
    let capacity =
        mutation
            .len()
            .checked_add(commit.len())
            .ok_or(FormatError::InvalidCatalogWal {
                detail: "transaction byte length overflows",
            })?;
    enforce_limit(
        "encoded transaction bytes",
        capacity as u64,
        MAX_CATALOG_WAL_REPLAY_BYTES,
    )?;
    let mut output = Vec::with_capacity(capacity);
    output.extend_from_slice(&mutation);
    output.extend_from_slice(&commit);
    Ok(output)
}

/// Append one transactional catalog mutation to the caller-owned WAL file.
/// The function owns framing and durability ordering, but deliberately does
/// not create a second catalog-only WAL namespace.
pub fn append_catalog_wal_transaction(
    wal: &mut File,
    transaction: &CatalogWalTransaction,
) -> FormatResult<u64> {
    let encoded = encode_catalog_wal_transaction(transaction)?;
    let mutation_bytes = usize::try_from(u64::from_le_bytes(
        encoded[16..24]
            .try_into()
            .expect("encoded record has a fixed length field"),
    ))
    .map_err(|_| invalid_error("mutation record length does not fit usize"))?;
    if mutation_bytes >= encoded.len() {
        return Err(invalid_error("encoded transaction has no commit marker"));
    }

    let append_start = wal
        .seek(SeekFrom::End(0))
        .map_err(|error| catalog_wal_io("seek append position", error))?;
    let mut commit_durable = false;
    let append = (|| {
        reach_generation_boundary(GenerationCrashPoint::WalBeforeRecordWrite)
            .map_err(|error| catalog_wal_io("inject before mutation write", error))?;
        wal.write_all(&encoded[..mutation_bytes])
            .map_err(|error| catalog_wal_io("write mutation record", error))?;
        reach_generation_boundary(GenerationCrashPoint::WalAfterRecordWriteBeforeSync)
            .map_err(|error| catalog_wal_io("inject after mutation write", error))?;
        wal.sync_data()
            .map_err(|error| catalog_wal_io("sync mutation record", error))?;
        reach_generation_boundary(GenerationCrashPoint::WalBeforeCommitMarker)
            .map_err(|error| catalog_wal_io("inject before commit marker", error))?;
        wal.write_all(&encoded[mutation_bytes..])
            .map_err(|error| catalog_wal_io("write commit marker", error))?;
        reach_generation_boundary(GenerationCrashPoint::WalAfterCommitMarkerWriteBeforeSync)
            .map_err(|error| catalog_wal_io("inject after commit-marker write", error))?;
        wal.sync_data()
            .map_err(|error| catalog_wal_io("sync commit marker", error))?;
        commit_durable = true;
        reach_generation_boundary(GenerationCrashPoint::WalCommitMarkerDurable)
            .map_err(|error| catalog_wal_io("inject after commit-marker durability", error))?;
        wal.seek(SeekFrom::End(0))
            .map_err(|error| catalog_wal_io("read durable append position", error))
    })();

    match append {
        Ok(position) => Ok(position),
        Err(error) if commit_durable => Err(error),
        Err(error) => {
            rollback_incomplete_append(wal, append_start)?;
            Err(error)
        }
    }
}

pub fn decode_catalog_wal(
    input: &[u8],
    limits: CatalogWalReplayLimits,
) -> FormatResult<CatalogWalReplay> {
    enforce_limit("stream bytes", input.len() as u64, limits.max_stream_bytes)?;
    let mut transactions = Vec::new();
    let mut cursor = 0_usize;
    let mut committed_bytes = 0_usize;
    while cursor < input.len() {
        if transactions.len() as u64 >= limits.max_transactions {
            return limit(
                "transaction count",
                transactions.len() as u64 + 1,
                limits.max_transactions,
            );
        }
        let transaction_start = cursor;
        let mutation = match read_record(&input[cursor..], limits)? {
            ReadRecord::Complete(record) => record,
            ReadRecord::Incomplete => {
                return Ok(replay_with_tail(
                    transactions,
                    committed_bytes,
                    input.len() - transaction_start,
                ));
            }
        };
        if mutation.kind != RecordKind::Mutation
            || mutation.sequence != 0
            || mutation.record_count != 0
            || mutation.payload.is_empty()
        {
            return invalid("transaction does not start with one mutation record");
        }
        cursor += mutation.length;
        let commit_bytes = input.len() - cursor;
        let commit = match read_record(&input[cursor..], limits) {
            Ok(ReadRecord::Complete(record)) => record,
            Ok(ReadRecord::Incomplete) => {
                return Ok(replay_with_tail(
                    transactions,
                    committed_bytes,
                    input.len() - transaction_start,
                ));
            }
            Err(_) if commit_bytes <= CATALOG_WAL_RECORD_HEADER_BYTES => {
                return Ok(replay_with_tail(
                    transactions,
                    committed_bytes,
                    input.len() - transaction_start,
                ));
            }
            Err(error) => return Err(error),
        };
        validate_commit_pair(&mutation, &commit)?;
        let mutation_set = decode_catalog_mutation_set(mutation.payload)?;
        if mutation_set.expected_catalog_generation() != mutation.expected_generation {
            return invalid("mutation payload expected generation differs from WAL header");
        }
        transactions.push(CatalogWalTransaction::new(
            mutation.transaction_id,
            mutation.successor_catalog_id,
            mutation.commit_lsn,
            mutation.created_unix_ns,
            mutation_set,
        )?);
        cursor += commit.length;
        committed_bytes = cursor;
    }
    Ok(replay_with_tail(transactions, committed_bytes, 0))
}

pub fn replay_catalog_wal(
    initial: &CatalogGeneration,
    replay: &CatalogWalReplay,
) -> FormatResult<CatalogGeneration> {
    replay_catalog_wal_after(initial, replay, 0)
}

/// Replay only transactions whose commit LSN is strictly newer than the
/// selected durable checkpoint floor. The complete decoded stream is still
/// checked for monotonic ordering so an older record cannot hide behind a
/// newer one.
pub fn replay_catalog_wal_after(
    initial: &CatalogGeneration,
    replay: &CatalogWalReplay,
    replay_floor_lsn: u64,
) -> FormatResult<CatalogGeneration> {
    replay_catalog_wal_after_owned(
        CatalogGeneration::new(initial.meta(), initial.graph().clone()),
        replay,
        replay_floor_lsn,
    )
}

/// Recovery variant that transfers ownership of the validated base catalog.
/// This avoids retaining two complete immutable graphs merely to enter replay.
pub(crate) fn replay_catalog_wal_after_owned(
    mut current: CatalogGeneration,
    replay: &CatalogWalReplay,
    replay_floor_lsn: u64,
) -> FormatResult<CatalogGeneration> {
    // A catalog pack is itself a durable snapshot.  A physical generation may
    // publish a newer pack without advancing the DML WAL replay floor (for
    // example, an ordinary seal following DDL).  Transactions already covered
    // by that pack must therefore be skipped even when CONTROL still retains
    // an older WAL floor.
    let effective_floor_lsn = replay_floor_lsn.max(current.meta().snapshot_lsn());
    let mut previous_lsn = None;
    for transaction in replay.transactions() {
        if previous_lsn.is_some_and(|previous| transaction.commit_lsn() <= previous) {
            return invalid("catalog WAL commit LSNs are not strictly increasing");
        }
        previous_lsn = Some(transaction.commit_lsn());
        if transaction.commit_lsn() <= effective_floor_lsn {
            continue;
        }
        let current_meta = current.meta();
        if transaction.commit_lsn() < current_meta.snapshot_lsn() {
            return invalid("catalog WAL commit LSN moves backwards");
        }
        let graph = transaction.mutation_set().apply(&current)?;
        let generation = transaction
            .mutation_set()
            .expected_catalog_generation()
            .checked_add(1)
            .ok_or(FormatError::InvalidCatalogWal {
                detail: "catalog generation overflows during replay",
            })?;
        let meta = CatalogPackMeta::new(
            current_meta.database_id(),
            transaction.successor_catalog_id(),
            generation,
            transaction.commit_lsn(),
            transaction.created_unix_ns(),
        )?;
        current = CatalogGeneration::new(meta, graph);
    }
    Ok(current)
}

#[allow(clippy::too_many_arguments)]
fn encode_record(
    transaction: &CatalogWalTransaction,
    kind: RecordKind,
    sequence: u32,
    record_count: u32,
    payload: &[u8],
    payload_sha256: [u8; 32],
) -> FormatResult<Vec<u8>> {
    let record_length = CATALOG_WAL_RECORD_HEADER_BYTES
        .checked_add(payload.len())
        .ok_or(FormatError::InvalidCatalogWal {
            detail: "record byte length overflows",
        })?;
    enforce_limit(
        "record bytes",
        record_length as u64,
        MAX_CATALOG_WAL_RECORD_BYTES,
    )?;
    let mut output = vec![0_u8; record_length];
    output[..8].copy_from_slice(MAGIC);
    put_u16(&mut output, 8, CATALOG_WAL_FORMAT_MAJOR)?;
    put_u16(&mut output, 10, CATALOG_WAL_FORMAT_MINOR)?;
    put_u32(&mut output, 12, CATALOG_WAL_RECORD_HEADER_BYTES as u32)?;
    put_u64(&mut output, 16, record_length as u64)?;
    output[24..40].copy_from_slice(&transaction.transaction_id().as_bytes());
    put_u16(&mut output, 40, kind as u16)?;
    put_u32(&mut output, 44, sequence)?;
    put_u64(
        &mut output,
        48,
        transaction.mutation_set().expected_catalog_generation(),
    )?;
    put_u64(&mut output, 56, payload.len() as u64)?;
    output[64..96].copy_from_slice(&payload_sha256);
    put_u32(&mut output, 96, radixdb_core::crc32_ieee(payload))?;
    output[104..120].copy_from_slice(&transaction.successor_catalog_id());
    put_u64(&mut output, 120, transaction.commit_lsn())?;
    put_u64(&mut output, 128, transaction.created_unix_ns())?;
    put_u32(&mut output, 136, record_count)?;
    output[CATALOG_WAL_RECORD_HEADER_BYTES..].copy_from_slice(payload);
    let header_crc32 = header_crc32(&output[..CATALOG_WAL_RECORD_HEADER_BYTES]);
    put_u32(&mut output, HEADER_CRC_OFFSET, header_crc32)?;
    Ok(output)
}

fn read_record(input: &[u8], limits: CatalogWalReplayLimits) -> FormatResult<ReadRecord<'_>> {
    if input.len() < CATALOG_WAL_RECORD_HEADER_BYTES {
        return Ok(ReadRecord::Incomplete);
    }
    let header = &input[..CATALOG_WAL_RECORD_HEADER_BYTES];
    if &header[..8] != MAGIC {
        return invalid("record magic is invalid");
    }
    if read_u16(header, 8)? != CATALOG_WAL_FORMAT_MAJOR
        || read_u16(header, 10)? != CATALOG_WAL_FORMAT_MINOR
    {
        return invalid("record format version is unsupported");
    }
    if read_u32(header, 12)? != CATALOG_WAL_RECORD_HEADER_BYTES as u32 {
        return invalid("record header length is invalid");
    }
    if read_u16(header, 42)? != 0
        || read_u32(header, 140)? != 0
        || header[144..].iter().any(|byte| *byte != 0)
    {
        return invalid("record flags/reserved fields are non-zero");
    }
    if header_crc32(header) != read_u32(header, HEADER_CRC_OFFSET)? {
        return Err(FormatError::CatalogWalChecksumMismatch {
            scope: "record header CRC32",
        });
    }
    let record_length = read_u64(header, 16)?;
    enforce_limit("record bytes", record_length, limits.max_record_bytes)?;
    if record_length < CATALOG_WAL_RECORD_HEADER_BYTES as u64 {
        return invalid("record length is shorter than its header");
    }
    let record_length =
        usize::try_from(record_length).map_err(|_| FormatError::CatalogWalLimitExceeded {
            field: "record bytes",
            actual: record_length,
            limit: usize::MAX as u64,
        })?;
    if record_length > input.len() {
        return Ok(ReadRecord::Incomplete);
    }
    let payload_length = read_u64(header, 56)?;
    if payload_length != (record_length - CATALOG_WAL_RECORD_HEADER_BYTES) as u64 {
        return invalid("record payload length disagrees with record length");
    }
    let payload = &input[CATALOG_WAL_RECORD_HEADER_BYTES..record_length];
    if radixdb_core::crc32_ieee(payload) != read_u32(header, 96)? {
        return Err(FormatError::CatalogWalChecksumMismatch {
            scope: "record payload CRC32",
        });
    }
    let payload_sha256 = read_array(header, 64)?;
    let kind = RecordKind::try_from(read_u16(header, 40)?)?;
    if kind == RecordKind::Mutation && radixdb_core::sha256_digest(payload) != payload_sha256 {
        return Err(FormatError::CatalogWalChecksumMismatch {
            scope: "mutation payload SHA-256",
        });
    }
    Ok(ReadRecord::Complete(Record {
        kind,
        transaction_id: CatalogWalTransactionId::from_bytes(read_array(header, 24)?)?,
        sequence: read_u32(header, 44)?,
        expected_generation: read_u64(header, 48)?,
        payload_sha256,
        successor_catalog_id: read_array(header, 104)?,
        commit_lsn: read_u64(header, 120)?,
        created_unix_ns: read_u64(header, 128)?,
        record_count: read_u32(header, 136)?,
        payload,
        length: record_length,
    }))
}

fn validate_commit_pair(mutation: &Record<'_>, commit: &Record<'_>) -> FormatResult<()> {
    if commit.kind != RecordKind::Commit
        || commit.sequence != 1
        || commit.record_count != 1
        || !commit.payload.is_empty()
        || commit.transaction_id != mutation.transaction_id
        || commit.expected_generation != mutation.expected_generation
        || commit.payload_sha256 != mutation.payload_sha256
        || commit.successor_catalog_id != mutation.successor_catalog_id
        || commit.commit_lsn != mutation.commit_lsn
        || commit.created_unix_ns != mutation.created_unix_ns
    {
        return invalid("commit marker does not bind the complete mutation record");
    }
    Ok(())
}

fn replay_with_tail(
    transactions: Vec<CatalogWalTransaction>,
    committed_bytes: usize,
    incomplete_tail_bytes: usize,
) -> CatalogWalReplay {
    CatalogWalReplay {
        transactions,
        committed_bytes,
        incomplete_tail_bytes,
    }
}

fn header_crc32(header: &[u8]) -> u32 {
    let mut canonical = [0_u8; CATALOG_WAL_RECORD_HEADER_BYTES];
    canonical.copy_from_slice(header);
    canonical[HEADER_CRC_OFFSET..HEADER_CRC_OFFSET + 4].fill(0);
    radixdb_core::crc32_ieee(&canonical)
}

fn require_runtime_limit(field: &'static str, value: u64, hard: u64) -> FormatResult<()> {
    if value > hard {
        return limit(field, value, hard);
    }
    Ok(())
}

fn enforce_limit(field: &'static str, actual: u64, limit_value: u64) -> FormatResult<()> {
    if actual > limit_value {
        return limit(field, actual, limit_value);
    }
    Ok(())
}

fn limit<T>(field: &'static str, actual: u64, limit: u64) -> FormatResult<T> {
    Err(FormatError::CatalogWalLimitExceeded {
        field,
        actual,
        limit,
    })
}

fn invalid<T>(detail: &'static str) -> FormatResult<T> {
    Err(FormatError::InvalidCatalogWal { detail })
}

fn invalid_error(detail: &'static str) -> FormatError {
    FormatError::InvalidCatalogWal { detail }
}

fn catalog_wal_io(operation: &'static str, error: std::io::Error) -> FormatError {
    FormatError::CatalogWalIo {
        operation,
        kind: error.kind(),
    }
}

fn rollback_incomplete_append(wal: &mut File, append_start: u64) -> FormatResult<()> {
    wal.set_len(append_start)
        .map_err(|error| catalog_wal_io("rollback incomplete append", error))?;
    wal.sync_data()
        .map_err(|error| catalog_wal_io("sync incomplete append rollback", error))?;
    wal.seek(SeekFrom::Start(append_start))
        .map_err(|error| catalog_wal_io("restore append position", error))?;
    Ok(())
}

fn read_array<const N: usize>(input: &[u8], offset: usize) -> FormatResult<[u8; N]> {
    input
        .get(offset..offset + N)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(FormatError::InvalidCatalogWal {
            detail: "fixed-width field is out of bounds",
        })
}

fn read_u16(input: &[u8], offset: usize) -> FormatResult<u16> {
    Ok(u16::from_le_bytes(read_array(input, offset)?))
}

fn read_u32(input: &[u8], offset: usize) -> FormatResult<u32> {
    Ok(u32::from_le_bytes(read_array(input, offset)?))
}

fn read_u64(input: &[u8], offset: usize) -> FormatResult<u64> {
    Ok(u64::from_le_bytes(read_array(input, offset)?))
}

fn put_u16(output: &mut [u8], offset: usize, value: u16) -> FormatResult<()> {
    put_bytes(output, offset, &value.to_le_bytes())
}

fn put_u32(output: &mut [u8], offset: usize, value: u32) -> FormatResult<()> {
    put_bytes(output, offset, &value.to_le_bytes())
}

fn put_u64(output: &mut [u8], offset: usize, value: u64) -> FormatResult<()> {
    put_bytes(output, offset, &value.to_le_bytes())
}

fn put_bytes(output: &mut [u8], offset: usize, value: &[u8]) -> FormatResult<()> {
    output
        .get_mut(offset..offset + value.len())
        .ok_or(FormatError::InvalidCatalogWal {
            detail: "fixed-width output field is out of bounds",
        })?
        .copy_from_slice(value);
    Ok(())
}
