use super::*;

/// Minimum payload size at which LZ4 compression is worth attempting.
const COMPRESSION_THRESHOLD: usize = 64;

/// WAL entry flags (stored in 1 byte)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WalFlags(pub(super) u8);

impl WalFlags {
    /// No flags set
    pub const NONE: WalFlags = WalFlags(0);
    /// Data is compressed (reserved for future use)
    pub const COMPRESSED: WalFlags = WalFlags(1 << 0);
    /// This is a commit record marker
    pub const COMMIT_MARKER: WalFlags = WalFlags(1 << 1);
    /// This is an abort record marker
    pub const ABORT_MARKER: WalFlags = WalFlags(1 << 2);
    /// This is a checkpoint record
    pub const CHECKPOINT_MARKER: WalFlags = WalFlags(1 << 3);
    /// This is a snapshot start marker
    pub const SNAPSHOT_START: WalFlags = WalFlags(1 << 4);
    /// This is a snapshot complete marker
    pub const SNAPSHOT_COMPLETE: WalFlags = WalFlags(1 << 5);
    /// This is a WAL rotation marker
    pub const ROTATION_MARKER: WalFlags = WalFlags(1 << 6);

    /// Create flags from raw byte
    pub fn from_byte(byte: u8) -> Self {
        WalFlags(byte)
    }

    /// Get raw byte value
    pub fn as_byte(&self) -> u8 {
        self.0
    }

    /// Check if a specific flag is set
    pub fn contains(&self, other: WalFlags) -> bool {
        (self.0 & other.0) == other.0
    }

    /// Set a flag
    pub fn set(&mut self, flag: WalFlags) {
        self.0 |= flag.0;
    }

    /// Clear a flag
    pub fn clear(&mut self, flag: WalFlags) {
        self.0 &= !flag.0;
    }

    /// Combine two flags
    pub fn union(self, other: WalFlags) -> WalFlags {
        WalFlags(self.0 | other.0)
    }
}

/// WAL operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WALOperationType {
    Insert = 1,
    Update = 2,
    Delete = 3,
    Commit = 4,
    Rollback = 5,
    TruncateTable = 13,
    /// One typed logical-catalog mutation set. Its payload is an encoded
    /// catalog WAL transaction whose commit LSN is the immediately following
    /// shared transaction marker in this same WAL authority.
    CatalogMutation = 15,
}

impl WALOperationType {
    /// Convert from u8
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(WALOperationType::Insert),
            2 => Some(WALOperationType::Update),
            3 => Some(WALOperationType::Delete),
            4 => Some(WALOperationType::Commit),
            5 => Some(WALOperationType::Rollback),
            13 => Some(WALOperationType::TruncateTable),
            15 => Some(WALOperationType::CatalogMutation),
            _ => None,
        }
    }

    /// Check if this is a DDL operation
    pub fn is_ddl(&self) -> bool {
        matches!(
            self,
            WALOperationType::TruncateTable | WALOperationType::CatalogMutation
        )
    }

    /// Check if this is a commit or rollback
    pub fn is_transaction_end(&self) -> bool {
        matches!(self, WALOperationType::Commit | WALOperationType::Rollback)
    }
}

/// WAL entry representing a single operation
#[derive(Debug, Clone)]
pub struct WALEntry {
    /// Log Sequence Number
    pub lsn: u64,
    /// Previous Log Sequence Number (for backward chaining)
    pub previous_lsn: u64,
    /// Entry flags
    pub flags: WalFlags,
    /// Transaction ID
    pub txn_id: i64,
    /// Stable catalog table identity for row-bearing operations.
    pub table_id: Option<ObjectId>,
    /// Row ID (0 for commits/rollbacks)
    pub row_id: i64,
    /// Operation type
    pub operation: WALOperationType,
    /// Serialized row data (empty for commits/rollbacks)
    pub data: Vec<u8>,
    /// Operation timestamp (nanoseconds since epoch)
    pub timestamp: i64,
}

/// Constructor boundary for the persisted stable table identity.
///
/// Product code can supply only `Option<ObjectId>`. Unit tests additionally
/// accept descriptive strings and deterministically turn them into opaque
/// IDs, keeping WAL mechanics readable without reintroducing a name-based
/// production format.
#[doc(hidden)]
pub struct WalTableIdentity(Option<ObjectId>);

impl From<Option<ObjectId>> for WalTableIdentity {
    fn from(value: Option<ObjectId>) -> Self {
        Self(value)
    }
}

#[cfg(test)]
impl From<String> for WalTableIdentity {
    fn from(value: String) -> Self {
        if value.is_empty() {
            return Self(None);
        }
        let mut hash = 0xcbf29ce484222325_u64;
        for byte in value.bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        let mut bytes = [0_u8; 16];
        bytes[..8].copy_from_slice(&hash.to_le_bytes());
        bytes[8..].copy_from_slice(&(!hash).to_le_bytes());
        Self(Some(
            ObjectId::from_user_bytes(bytes).expect("test table identity is user-allocatable"),
        ))
    }
}

impl WALEntry {
    /// Create a new WAL entry
    pub fn new(
        txn_id: i64,
        table_id: impl Into<WalTableIdentity>,
        row_id: i64,
        operation: WALOperationType,
        data: Vec<u8>,
    ) -> Self {
        let timestamp = system_time_now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);

        Self {
            lsn: 0,          // Will be assigned by WALManager
            previous_lsn: 0, // Will be assigned by WALManager
            flags: WalFlags::NONE,
            txn_id,
            table_id: table_id.into().0,
            row_id,
            operation,
            data,
            timestamp,
        }
    }

    /// Create a new WAL entry with flags
    pub fn with_flags(
        txn_id: i64,
        table_id: impl Into<WalTableIdentity>,
        row_id: i64,
        operation: WALOperationType,
        data: Vec<u8>,
        flags: WalFlags,
    ) -> Self {
        let mut entry = Self::new(txn_id, table_id, row_id, operation, data);
        entry.flags = flags;
        entry
    }

    /// Create a commit entry (with COMMIT_MARKER flag for two-phase recovery)
    pub fn commit(txn_id: i64) -> Self {
        // Always set COMMIT_MARKER flag so two-phase recovery can identify commits
        Self::with_flags(
            txn_id,
            None,
            0,
            WALOperationType::Commit,
            Vec::new(),
            WalFlags::COMMIT_MARKER,
        )
    }

    /// Create a commit marker entry (explicit commit record for two-phase recovery)
    /// Note: This is now equivalent to commit() - kept for API compatibility
    pub fn commit_marker(txn_id: i64) -> Self {
        Self::commit(txn_id)
    }

    /// Create a rollback entry (with ABORT_MARKER flag for two-phase recovery)
    pub fn rollback(txn_id: i64) -> Self {
        // Always set ABORT_MARKER flag so two-phase recovery can identify aborts
        Self::with_flags(
            txn_id,
            None,
            0,
            WALOperationType::Rollback,
            Vec::new(),
            WalFlags::ABORT_MARKER,
        )
    }

    /// Create an abort marker entry (explicit abort record for two-phase recovery)
    /// Note: This is now equivalent to rollback() - kept for API compatibility
    pub fn abort_marker(txn_id: i64) -> Self {
        Self::rollback(txn_id)
    }

    /// Check if this entry is a commit marker
    pub fn is_commit_marker(&self) -> bool {
        self.flags.contains(WalFlags::COMMIT_MARKER) || self.operation == WALOperationType::Commit
    }

    /// Check if this entry is an abort marker
    pub fn is_abort_marker(&self) -> bool {
        self.flags.contains(WalFlags::ABORT_MARKER) || self.operation == WALOperationType::Rollback
    }

    /// Encode entry to binary format with integrity protection
    ///
    /// Format V4 (the same 32-byte header framing):
    /// ┌─────────────────────────────────────────────────────────────────┐
    /// │ HEADER (32 bytes)                                               │
    /// ├─────────────────────────────────────────────────────────────────┤
    /// │ Magic          (4 bytes)  0x454C4157 "WALE"                     │
    /// │ Version        (1 byte)   Format version (currently 3)          │
    /// │ Flags          (1 byte)   Entry flags                           │
    /// │ Header Size    (2 bytes)  Total header size (32)                │
    /// │ LSN            (8 bytes)  Log sequence number                   │
    /// │ Previous LSN   (8 bytes)  Previous entry LSN (chain link)       │
    /// │ Entry Size     (4 bytes)  Size of data payload                  │
    /// │ Reserved       (4 bytes)  Reserved for future use               │
    /// ├─────────────────────────────────────────────────────────────────┤
    /// │ DATA PORTION (variable):                                        │
    /// │   - TxnID (8 bytes)                                             │
    /// │   - Table ObjectId (16 bytes; zero for non-table records)       │
    /// │   - RowID (8 bytes)                                             │
    /// │   - Operation (1 byte)                                          │
    /// │   - Timestamp (8 bytes)                                         │
    /// │   - DataLen (4 bytes) + Data                                    │
    /// ├─────────────────────────────────────────────────────────────────┤
    /// │ CRC32 (4 bytes): checksum of header + data                      │
    /// └─────────────────────────────────────────────────────────────────┘
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate_semantics()?;

        if self.data.len() > MAX_WAL_RECORD_DATA_SIZE {
            return Err(Error::internal(format!(
                "WAL logical payload is too large: {} bytes (maximum {})",
                self.data.len(),
                MAX_WAL_RECORD_DATA_SIZE
            )));
        }

        // Determine if we should compress the data payload.
        // Avoid cloning self.data for the uncompressed case — write it
        // directly into buf via extend_from_slice instead.
        let compressed_data: Option<Vec<u8>>;
        let use_compression;
        if self.data.len() >= COMPRESSION_THRESHOLD {
            let compressed = lz4_flex::compress_prepend_size(&self.data);
            if compressed.len() < self.data.len() {
                compressed_data = Some(compressed);
                use_compression = true;
            } else {
                compressed_data = None;
                use_compression = false;
            }
        } else {
            compressed_data = None;
            use_compression = false;
        }
        let payload: &[u8] = compressed_data.as_deref().unwrap_or(&self.data);

        // Calculate data portion size: txnID(8) + table ObjectId(16) +
        // rowID(8) + op(1) + timestamp(8) + dataLen(4) + data.
        let data_size = MIN_WAL_RECORD_DATA_SIZE
            .checked_add(payload.len())
            .ok_or_else(|| Error::internal("WAL record size overflow"))?;
        if data_size > MAX_WAL_RECORD_DATA_SIZE {
            return Err(Error::internal(format!(
                "encoded WAL record is too large: {} bytes (maximum {})",
                data_size, MAX_WAL_RECORD_DATA_SIZE
            )));
        }
        let data_size_u32 = u32::try_from(data_size)
            .map_err(|_| Error::internal("encoded WAL record does not fit u32"))?;

        // Total buffer: header(32) + data + CRC(4)
        let mut buf = Vec::with_capacity(WAL_HEADER_SIZE as usize + data_size + 4);

        // ========== HEADER (32 bytes) ==========
        // Magic marker (4 bytes)
        buf.extend_from_slice(&WAL_ENTRY_MAGIC.to_le_bytes());

        // Version (1 byte)
        buf.push(WAL_FORMAT_VERSION);

        // Flags (1 byte) - set COMPRESSED if using compression
        let mut flags = self.flags;
        // Compression is a property of the bytes emitted by this call, not a
        // caller-controlled semantic flag.
        flags.clear(WalFlags::COMPRESSED);
        if use_compression {
            flags.set(WalFlags::COMPRESSED);
        }
        buf.push(flags.as_byte());

        // Header Size (2 bytes)
        buf.extend_from_slice(&WAL_HEADER_SIZE.to_le_bytes());

        // LSN (8 bytes)
        buf.extend_from_slice(&self.lsn.to_le_bytes());

        // Previous LSN (8 bytes)
        buf.extend_from_slice(&self.previous_lsn.to_le_bytes());

        // Entry Size (4 bytes) - size of data portion only
        buf.extend_from_slice(&data_size_u32.to_le_bytes());

        // Reserved (4 bytes)
        buf.extend_from_slice(&[0u8; 4]);

        // ========== DATA PORTION ==========
        // TxnID (8 bytes)
        buf.extend_from_slice(&self.txn_id.to_le_bytes());

        // Stable table identity. Non-table records use the reserved all-zero
        // envelope value, which can never be a valid catalog ObjectId.
        buf.extend_from_slice(
            self.table_id
                .as_ref()
                .map(ObjectId::as_bytes)
                .unwrap_or(&[0_u8; 16]),
        );

        // RowID (8 bytes)
        buf.extend_from_slice(&self.row_id.to_le_bytes());

        // Operation (1 byte)
        buf.push(self.operation as u8);

        // Timestamp (8 bytes)
        buf.extend_from_slice(&self.timestamp.to_le_bytes());

        // Data length (4 bytes) + data (possibly compressed)
        // When compressed, lz4_flex::compress_prepend_size includes the original size
        let payload_len = u32::try_from(payload.len())
            .map_err(|_| Error::internal("WAL payload does not fit u32"))?;
        buf.extend_from_slice(&payload_len.to_le_bytes());
        buf.extend_from_slice(payload);

        // ========== CRC32 (4 bytes) ==========
        // V4 protects every semantic/framing header field as well as the data.
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());

        Ok(buf)
    }

    /// Decode a current-version entry from its data portion after the caller
    /// has parsed the semantic header fields.
    ///
    /// Parameters:
    /// - lsn, previous_lsn, flags: extracted from header by caller
    /// - data: data portion + CRC (4 bytes)
    ///
    pub fn decode(lsn: u64, previous_lsn: u64, flags: WalFlags, data: &[u8]) -> Result<Self> {
        if data.len() < 4 {
            return Err(Error::internal("data too short for WAL checksum"));
        }
        let header =
            Self::checksum_header(WAL_FORMAT_VERSION, flags, lsn, previous_lsn, data.len() - 4)?;
        Self::decode_with_crc_prefix(lsn, previous_lsn, flags, data, &header)
    }

    pub(super) fn decode_versioned(
        _version: u8,
        header: &[u8; WAL_HEADER_SIZE as usize],
        lsn: u64,
        previous_lsn: u64,
        flags: WalFlags,
        data: &[u8],
    ) -> Result<Self> {
        Self::decode_with_crc_prefix(lsn, previous_lsn, flags, data, header)
    }

    fn checksum_header(
        version: u8,
        flags: WalFlags,
        lsn: u64,
        previous_lsn: u64,
        entry_size: usize,
    ) -> Result<[u8; WAL_HEADER_SIZE as usize]> {
        let entry_size = u32::try_from(entry_size)
            .map_err(|_| Error::internal("encoded WAL record does not fit u32"))?;
        let mut header = [0u8; WAL_HEADER_SIZE as usize];
        header[0..4].copy_from_slice(&WAL_ENTRY_MAGIC.to_le_bytes());
        header[4] = version;
        header[5] = flags.as_byte();
        header[6..8].copy_from_slice(&WAL_HEADER_SIZE.to_le_bytes());
        header[8..16].copy_from_slice(&lsn.to_le_bytes());
        header[16..24].copy_from_slice(&previous_lsn.to_le_bytes());
        header[24..28].copy_from_slice(&entry_size.to_le_bytes());
        Ok(header)
    }

    fn decode_with_crc_prefix(
        lsn: u64,
        previous_lsn: u64,
        flags: WalFlags,
        data: &[u8],
        crc_prefix: &[u8],
    ) -> Result<Self> {
        if data.len() > MAX_WAL_RECORD_DATA_SIZE + 4 {
            return Err(Error::internal(format!(
                "encoded WAL record exceeds limit at LSN {}: {} bytes",
                lsn,
                data.len()
            )));
        }
        // Minimum size: fixed V4 data portion plus CRC32.
        if data.len() < MIN_WAL_RECORD_DATA_SIZE + 4 {
            return Err(Error::internal(format!(
                "data too short for WAL entry: {} bytes",
                data.len()
            )));
        }

        // Verify CRC32 (last 4 bytes)
        let crc_offset = data.len() - 4;
        let stored_crc = u32::from_le_bytes(data[crc_offset..].try_into().unwrap());
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(crc_prefix);
        hasher.update(&data[..crc_offset]);
        let computed_crc = hasher.finalize();

        #[cfg(feature = "test-mutations")]
        let checksum_verification_disabled =
            crate::test_mutations::checksum_verification_disabled();
        #[cfg(not(feature = "test-mutations"))]
        let checksum_verification_disabled = false;

        if stored_crc != computed_crc && !checksum_verification_disabled {
            return Err(Error::internal(format!(
                "WAL entry checksum mismatch at LSN {}: stored={:#x}, computed={:#x}",
                lsn, stored_crc, computed_crc
            )));
        }

        // Data portion (excluding CRC)
        let data = &data[..crc_offset];
        let mut pos = 0;

        // TxnID (8 bytes)
        if pos + 8 > data.len() {
            return Err(Error::internal("unexpected end of data reading txn_id"));
        }
        let txn_id = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;

        // Stable table ObjectId. The all-zero value is reserved for records
        // that have no table owner.
        let table_id_bytes: [u8; 16] = data[pos..pos + 16]
            .try_into()
            .expect("minimum V4 WAL size was checked");
        pos += 16;
        let table_id = if table_id_bytes == [0_u8; 16] {
            None
        } else {
            Some(ObjectId::from_bytes(table_id_bytes).map_err(|error| {
                Error::internal(format!("invalid WAL table ObjectId at LSN {lsn}: {error}"))
            })?)
        };

        // RowID (8 bytes)
        if pos + 8 > data.len() {
            return Err(Error::internal("unexpected end of data reading row_id"));
        }
        let row_id = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;

        // Operation (1 byte)
        if pos + 1 > data.len() {
            return Err(Error::internal("unexpected end of data reading operation"));
        }
        let operation = WALOperationType::from_u8(data[pos])
            .ok_or_else(|| Error::internal(format!("invalid operation type: {}", data[pos])))?;
        pos += 1;

        // Timestamp (8 bytes)
        if pos + 8 > data.len() {
            return Err(Error::internal("unexpected end of data reading timestamp"));
        }
        let timestamp = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;

        // Data length (4 bytes)
        if pos + 4 > data.len() {
            return Err(Error::internal(
                "unexpected end of data reading data length",
            ));
        }
        let data_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        // Data
        if pos.checked_add(data_len).is_none_or(|end| end > data.len()) {
            return Err(Error::internal("unexpected end of data reading data"));
        }
        let raw_data = &data[pos..pos + data_len];
        pos += data_len;
        if pos != data.len() {
            return Err(Error::internal(format!(
                "WAL entry has {} trailing data bytes at LSN {}",
                data.len() - pos,
                lsn
            )));
        }

        // Decompress if COMPRESSED flag is set
        let entry_data = if flags.contains(WalFlags::COMPRESSED) {
            if raw_data.len() < 4 {
                return Err(Error::internal(
                    "compressed WAL payload is missing its decoded-size prefix",
                ));
            }
            let decoded_size = u32::from_le_bytes(raw_data[..4].try_into().unwrap()) as usize;
            if decoded_size > MAX_WAL_RECORD_DATA_SIZE {
                return Err(Error::internal(format!(
                    "decoded WAL payload exceeds limit at LSN {}: {} bytes (maximum {})",
                    lsn, decoded_size, MAX_WAL_RECORD_DATA_SIZE
                )));
            }
            lz4_flex::decompress_size_prepended(raw_data).map_err(|e| {
                Error::internal(format!("failed to decompress WAL entry data: {}", e))
            })?
        } else {
            raw_data.to_vec()
        };

        let entry = WALEntry {
            lsn,
            previous_lsn,
            flags,
            txn_id,
            table_id,
            row_id,
            operation,
            data: entry_data,
            timestamp,
        };
        entry.validate_semantics()?;
        Ok(entry)
    }

    fn validate_semantics(&self) -> Result<()> {
        if self.flags.as_byte() & !WAL_KNOWN_FLAGS != 0 {
            return Err(Error::internal(format!(
                "WAL entry contains unknown flags: {:#x}",
                self.flags.as_byte()
            )));
        }

        let commit = self.flags.contains(WalFlags::COMMIT_MARKER);
        let abort = self.flags.contains(WalFlags::ABORT_MARKER);
        if commit && abort {
            return Err(Error::internal(
                "WAL entry cannot be both commit and abort marker",
            ));
        }

        match self.operation {
            WALOperationType::Commit if !commit || abort => {
                return Err(Error::internal("WAL commit operation/marker flag mismatch"));
            }
            WALOperationType::Rollback if !abort || commit => {
                return Err(Error::internal(
                    "WAL rollback operation/marker flag mismatch",
                ));
            }
            WALOperationType::Commit | WALOperationType::Rollback => {
                if self.table_id.is_some() || self.row_id != 0 || !self.data.is_empty() {
                    return Err(Error::internal(
                        "WAL transaction outcome marker contains row payload",
                    ));
                }
            }
            _ if commit || abort => {
                return Err(Error::internal(
                    "WAL data operation carries transaction outcome flag",
                ));
            }
            _ => {}
        }

        let requires_table_id = matches!(
            self.operation,
            WALOperationType::Insert
                | WALOperationType::Update
                | WALOperationType::Delete
                | WALOperationType::TruncateTable
        );
        if requires_table_id != self.table_id.is_some() {
            return Err(Error::internal(format!(
                "WAL operation {:?} has invalid table identity presence",
                self.operation
            )));
        }

        Ok(())
    }
}
