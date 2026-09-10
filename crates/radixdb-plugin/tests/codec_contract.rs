use radixdb_plugin::prelude::*;

#[derive(Debug, Default, Clone, RadixType)]
#[radix_type(
    id = "fixed_fields",
    name = "fixed_fields",
    codec = 1,
    semantic_revision = 1,
    storage = "fixed",
    max_bytes = 21
)]
struct FixedFields {
    #[radix_field(codec = "i32-le")]
    signed: i32,
    #[radix_field(codec = "f64-le")]
    floating: f64,
    #[radix_field(codec = "u16-le")]
    fixed: [u16; 4],
    #[radix_field(codec = "bool-u8")]
    enabled: bool,
}

#[derive(Debug, Default, Clone, RadixType)]
#[radix_type(
    id = "bounded_sequence",
    name = "bounded_sequence",
    codec = 1,
    semantic_revision = 1,
    storage = "variable",
    max_bytes = 36
)]
struct BoundedSequence {
    #[radix_field(codec = "u32-le", max_items = 8, max_bytes = 36)]
    values: Vec<u32>,
}

#[derive(Debug, Clone)]
struct ManualShape {
    tag: u8,
    payload: Vec<u8>,
}

struct ManualShapeCodec;

impl ManualCodec<ManualShape> for ManualShapeCodec {
    fn encode(value: &ManualShape, output: &mut CodecWriter) -> PluginResult<()> {
        if value.payload.len() > 27 {
            return Err(PluginError::limit_exceeded("manual payload exceeds bound"));
        }
        output.write(&[value.tag, value.payload.len() as u8])?;
        output.write(&value.payload)
    }

    fn decode(input: &mut CodecReader<'_>) -> PluginResult<ManualShape> {
        let header = input.read(2)?;
        let length = header[1] as usize;
        if length > 27 {
            return Err(PluginError::limit_exceeded("manual payload exceeds bound"));
        }
        Ok(ManualShape {
            tag: header[0],
            payload: input.read(length)?.to_vec(),
        })
    }

    fn corpus() -> Vec<ManualShape> {
        vec![
            ManualShape {
                tag: 0,
                payload: Vec::new(),
            },
            ManualShape {
                tag: u8::MAX,
                payload: vec![0, 1, 2, u8::MAX],
            },
        ]
    }
}

#[derive(Debug, Clone, RadixType)]
#[radix_type(
    id = "manual_shape",
    name = "manual_shape",
    codec = 1,
    semantic_revision = 1,
    storage = "variable",
    max_bytes = 29,
    manual = ManualShapeCodec
)]
struct ManualShapeType {
    tag: u8,
    payload: Vec<u8>,
}

impl ManualCodec<ManualShapeType> for ManualShapeCodec {
    fn encode(value: &ManualShapeType, output: &mut CodecWriter) -> PluginResult<()> {
        <Self as ManualCodec<ManualShape>>::encode(
            &ManualShape {
                tag: value.tag,
                payload: value.payload.clone(),
            },
            output,
        )
    }

    fn decode(input: &mut CodecReader<'_>) -> PluginResult<ManualShapeType> {
        let value = <Self as ManualCodec<ManualShape>>::decode(input)?;
        Ok(ManualShapeType {
            tag: value.tag,
            payload: value.payload,
        })
    }

    fn corpus() -> Vec<ManualShapeType> {
        <Self as ManualCodec<ManualShape>>::corpus()
            .into_iter()
            .map(|value| ManualShapeType {
                tag: value.tag,
                payload: value.payload,
            })
            .collect()
    }
}

#[derive(Debug, Default, Clone, RadixType)]
#[radix_type(
    id = "layout_probe",
    name = "layout_probe_i32",
    codec = 1,
    semantic_revision = 1,
    storage = "fixed",
    max_bytes = 4
)]
struct LayoutI32 {
    #[radix_field(codec = "i32-le")]
    value: i32,
}

#[derive(Debug, Default, Clone, RadixType)]
#[radix_type(
    id = "layout_probe",
    name = "layout_probe_i64",
    codec = 1,
    semantic_revision = 1,
    storage = "fixed",
    max_bytes = 8
)]
struct LayoutI64 {
    #[radix_field(codec = "i64-le")]
    value: i64,
}

#[test]
fn generated_and_manual_codecs_share_the_same_gate() {
    let fixed = radixdb_plugin::testing::check_type::<FixedFields>().unwrap();
    let sequence = radixdb_plugin::testing::check_type::<BoundedSequence>().unwrap();
    let manual = radixdb_plugin::testing::check_type::<ManualShapeType>().unwrap();
    assert!(fixed.corpus_values > 1);
    assert!(sequence.corpus_values > 1);
    assert_eq!(manual.corpus_values, 2);
}

#[test]
fn float_field_codec_preserves_exact_ieee_bits() {
    let report = radixdb_plugin::testing::check_type::<FixedFields>().unwrap();
    assert!(report.codec_vectors.iter().any(|bytes| {
        let bits = u64::from_le_bytes(bytes[4..12].try_into().unwrap());
        bits == 0x7ff8_0000_0000_0001
    }));
    assert!(report.codec_vectors.iter().any(|bytes| {
        let bits = u64::from_le_bytes(bytes[4..12].try_into().unwrap());
        bits == (-0.0_f64).to_bits()
    }));
}

#[test]
fn incompatible_layout_changes_codec_fingerprint() {
    assert_ne!(LayoutI32::CODEC_FINGERPRINT, LayoutI64::CODEC_FINGERPRINT);
}

#[test]
fn malformed_manual_and_sequence_payloads_fail_closed() {
    let malformed: &[&[u8]] = &[&[], &[1], &[1, 30], &[0, 0, 0, 9]];
    assert!(
        radixdb_plugin::testing::fuzz_malformed_external_bytes::<ManualShapeType>(malformed) >= 3
    );
    assert!(
        radixdb_plugin::testing::fuzz_malformed_external_bytes::<BoundedSequence>(malformed) >= 3
    );
}
