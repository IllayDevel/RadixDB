use radixdb_storage::v6::{
    decode_control_slot, encode_control_slot, select_control_slots, CatalogGeneration, CatalogId,
    CatalogRootRef, ControlRecord, ControlSlotIndex, DatabaseGeneration, DatabaseId,
    DatabaseManifestRootRef, FormatError, ManifestGeneration, ManifestId, WalGeneration,
    WalReplayFloor, WriterInstanceId, CONTROL_RECORD_BYTES,
};

fn identity(marker: u8) -> [u8; 16] {
    [marker; 16]
}

fn record(slot: ControlSlotIndex, generation: u64, marker: u8) -> ControlRecord {
    ControlRecord::new(
        slot,
        DatabaseGeneration::new(generation).unwrap(),
        DatabaseId::from_bytes(identity(marker)).unwrap(),
        DatabaseManifestRootRef::new(
            ManifestId::from_bytes(identity(marker.wrapping_add(1))).unwrap(),
            ManifestGeneration::new(generation).unwrap(),
            [marker.wrapping_add(2); 32],
        ),
        CatalogRootRef::new(
            CatalogId::from_bytes(identity(marker.wrapping_add(3))).unwrap(),
            CatalogGeneration::new(generation + 10).unwrap(),
            [marker.wrapping_add(4); 32],
        ),
        WalReplayFloor::new(
            WalGeneration::new(generation + 20).unwrap(),
            generation * 100,
        ),
        generation * 1_000,
        WriterInstanceId::from_bytes(identity(marker.wrapping_add(5))).unwrap(),
    )
    .unwrap()
}

fn refresh_crc(bytes: &mut [u8]) {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&bytes[..248]);
    hasher.update(&bytes[252..]);
    bytes[248..252].copy_from_slice(&hasher.finalize().to_le_bytes());
}

#[test]
fn control_codec_has_exact_layout_and_roundtrips() {
    let expected = record(ControlSlotIndex::One, 7, 0x11);
    let bytes = encode_control_slot(expected);

    assert_eq!(bytes.len(), CONTROL_RECORD_BYTES);
    assert_eq!(&bytes[..8], b"RDX6CTL\0");
    assert_eq!(u16::from_le_bytes(bytes[8..10].try_into().unwrap()), 6);
    assert_eq!(u16::from_le_bytes(bytes[10..12].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(bytes[12..16].try_into().unwrap()), 4096);
    assert_eq!(bytes[16], 1);
    assert_eq!(bytes[17], 1);
    assert_eq!(u64::from_le_bytes(bytes[24..32].try_into().unwrap()), 7);
    assert_eq!(&bytes[32..48], &[0x11; 16]);
    assert_eq!(&bytes[48..64], &[0x12; 16]);
    assert_eq!(u64::from_le_bytes(bytes[64..72].try_into().unwrap()), 7);
    assert_eq!(&bytes[72..88], &[0x14; 16]);
    assert_eq!(u64::from_le_bytes(bytes[88..96].try_into().unwrap()), 17);
    assert_eq!(u64::from_le_bytes(bytes[96..104].try_into().unwrap()), 27);
    assert_eq!(u64::from_le_bytes(bytes[104..112].try_into().unwrap()), 700);
    assert_eq!(
        u64::from_le_bytes(bytes[112..120].try_into().unwrap()),
        7_000
    );
    assert_eq!(&bytes[128..160], &[0x13; 32]);
    assert_eq!(&bytes[160..192], &[0x15; 32]);
    assert_eq!(&bytes[192..208], &[0x16; 16]);
    assert!(bytes[208..248].iter().all(|byte| *byte == 0));
    assert!(bytes[252..].iter().all(|byte| *byte == 0));
    assert_ne!(&bytes[248..252], &[0; 4]);

    assert_eq!(
        decode_control_slot(&bytes, ControlSlotIndex::One).unwrap(),
        expected
    );
}

#[test]
fn control_decoder_rejects_torn_unknown_and_mismatched_records() {
    let original = encode_control_slot(record(ControlSlotIndex::Zero, 1, 0x21));
    assert!(decode_control_slot(&original[..4095], ControlSlotIndex::Zero).is_err());
    let mut extra = original.to_vec();
    extra.push(0);
    assert!(decode_control_slot(&extra, ControlSlotIndex::Zero).is_err());
    assert!(decode_control_slot(&original, ControlSlotIndex::One).is_err());

    let mut bad_crc = original;
    bad_crc[32] ^= 1;
    assert_eq!(
        decode_control_slot(&bad_crc, ControlSlotIndex::Zero),
        Err(FormatError::ControlChecksumMismatch)
    );

    let mut reserved = original;
    reserved[3000] = 1;
    refresh_crc(&mut reserved);
    assert!(matches!(
        decode_control_slot(&reserved, ControlSlotIndex::Zero),
        Err(FormatError::InvalidControlRecord { .. })
    ));

    let mut flags = original;
    flags[120..128].copy_from_slice(&1_u64.to_le_bytes());
    refresh_crc(&mut flags);
    assert!(matches!(
        decode_control_slot(&flags, ControlSlotIndex::Zero),
        Err(FormatError::InvalidControlRecord { .. })
    ));

    let mut zero_database = original;
    zero_database[32..48].fill(0);
    refresh_crc(&mut zero_database);
    assert!(matches!(
        decode_control_slot(&zero_database, ControlSlotIndex::Zero),
        Err(FormatError::ZeroIdentity { .. })
    ));
}

#[test]
fn selector_falls_back_from_newer_incomplete_or_invalid_slot() {
    let old = encode_control_slot(record(ControlSlotIndex::Zero, 4, 0x31));
    let new = encode_control_slot(record(ControlSlotIndex::One, 5, 0x41));

    let selected = select_control_slots(&old, &new, |candidate| {
        candidate.database_generation().get() == 4
    })
    .unwrap();
    assert_eq!(selected.database_generation().get(), 4);

    let selected = select_control_slots(&old, &new, |_| true).unwrap();
    assert_eq!(selected.database_generation().get(), 5);

    let selected = select_control_slots(&old, &new[..4090], |_| true).unwrap();
    assert_eq!(selected.database_generation().get(), 4);

    assert_eq!(
        select_control_slots(&old, &new, |_| false),
        Err(FormatError::NoCompleteControlGeneration)
    );
    assert_eq!(
        select_control_slots(&old[..10], &new[..10], |_| true),
        Err(FormatError::NoValidControlSlot)
    );
}

#[test]
fn equal_generation_with_different_roots_is_split_brain() {
    let left = encode_control_slot(record(ControlSlotIndex::Zero, 9, 0x51));
    let right = encode_control_slot(record(ControlSlotIndex::One, 9, 0x61));
    assert_eq!(
        select_control_slots(&left, &right, |_| true),
        Err(FormatError::ControlSplitBrain { generation: 9 })
    );
}

#[test]
fn control_constructor_rejects_manifest_generation_mismatch() {
    let result = ControlRecord::new(
        ControlSlotIndex::Zero,
        DatabaseGeneration::new(3).unwrap(),
        DatabaseId::from_bytes(identity(1)).unwrap(),
        DatabaseManifestRootRef::new(
            ManifestId::from_bytes(identity(2)).unwrap(),
            ManifestGeneration::new(2).unwrap(),
            [3; 32],
        ),
        CatalogRootRef::new(
            CatalogId::from_bytes(identity(4)).unwrap(),
            CatalogGeneration::new(5).unwrap(),
            [6; 32],
        ),
        WalReplayFloor::new(WalGeneration::new(7).unwrap(), 8),
        9,
        WriterInstanceId::from_bytes(identity(10)).unwrap(),
    );
    assert!(matches!(
        result,
        Err(FormatError::InvalidControlRecord { .. })
    ));
}
