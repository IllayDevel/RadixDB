use super::super::{FormatError, FormatResult};
use super::model::{invalid, DataBlockRef, DataPhysicalCodec};

pub(crate) fn encode_physical(logical: &[u8], codec: DataPhysicalCodec) -> Vec<u8> {
    match codec {
        DataPhysicalCodec::None => logical.to_vec(),
        DataPhysicalCodec::Lz4 => lz4_flex::block::compress(logical),
    }
}

pub(crate) fn decode_physical(stored: &[u8], block: &DataBlockRef) -> FormatResult<Vec<u8>> {
    let started = std::time::Instant::now();
    let logical = match block.codec() {
        DataPhysicalCodec::None => {
            if stored.len() as u64 != block.logical_length() {
                return Err(invalid("uncompressed block length mismatch"));
            }
            Ok(stored.to_vec())
        }
        DataPhysicalCodec::Lz4 => {
            let logical_length = usize::try_from(block.logical_length())
                .map_err(|_| invalid("logical block length does not fit this platform"))?;
            lz4_flex::block::decompress(stored, logical_length).map_err(|_| {
                FormatError::InvalidDataArtifact {
                    detail: "raw LZ4 block cannot be decompressed to declared length",
                }
            })
        }
    }?;
    crate::instrumentation::record_decompression(
        stored.len() as u64,
        logical.len() as u64,
        started.elapsed(),
    );
    Ok(logical)
}
