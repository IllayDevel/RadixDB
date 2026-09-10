use crate::{CatalogError, CatalogResult};

pub const FORMAT_MAJOR: u16 = 6;
pub const BASELINE_FORMAT_MINOR: u16 = crate::BASELINE_CATALOG_MINOR;
pub const LATEST_FORMAT_MINOR: u16 = crate::LATEST_CATALOG_MINOR;
pub const CATALOG_MAGIC: &[u8; 8] = b"RDX6CAT\0";
pub const PAYLOAD_MAGIC: &[u8; 4] = b"COBJ";
pub const FOOTER_MAGIC: &[u8; 8] = b"RDX6END\0";
pub const HEADER_BYTES: usize = 256;
pub const OBJECT_ENTRY_BYTES: usize = 160;
pub const EDGE_ENTRY_BYTES: usize = 48;
pub const PAYLOAD_HEADER_BYTES: usize = 32;
pub const FIELD_ENTRY_BYTES: usize = 24;
pub const FOOTER_BYTES: usize = 48;

pub const MAX_CATALOG_FILE_BYTES: u64 = 512 * 1024 * 1024;
pub const MAX_CATALOG_OBJECTS: u64 = 262_144;
pub const MAX_CATALOG_EDGES: u64 = 1_048_576;
pub const MAX_PAYLOAD_AREA_BYTES: u64 = 256 * 1024 * 1024;
pub const MAX_STRING_AREA_BYTES: u64 = 128 * 1024 * 1024;
pub const MAX_FIELDS_PER_OBJECT: u64 = 64;
pub const MAX_PAYLOAD_BYTES_PER_OBJECT: u64 = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobRef {
    pub offset: u64,
    pub length: u32,
    pub crc32: u32,
}

impl BlobRef {
    pub const fn absent() -> Self {
        Self {
            offset: 0,
            length: 0,
            crc32: 0,
        }
    }

    pub fn write_at(self, output: &mut [u8], offset: usize) -> CatalogResult<()> {
        put_u64(output, offset, self.offset)?;
        put_u32(output, offset + 8, self.length)?;
        put_u32(output, offset + 12, self.crc32)
    }

    pub fn read_at(input: &[u8], offset: usize) -> CatalogResult<Self> {
        Ok(Self {
            offset: read_u64(input, offset)?,
            length: read_u32(input, offset + 8)?,
            crc32: read_u32(input, offset + 12)?,
        })
    }
}

pub fn checked_usize(value: u64, field: &'static str) -> CatalogResult<usize> {
    usize::try_from(value).map_err(|_| CatalogError::CatalogLimitExceeded {
        field,
        actual: value,
        limit: usize::MAX as u64,
    })
}

pub fn checked_product(count: u64, width: usize, field: &'static str) -> CatalogResult<usize> {
    let bytes = count
        .checked_mul(width as u64)
        .ok_or(CatalogError::InvalidCatalogFormat {
            detail: "directory length multiplication overflow",
        })?;
    checked_usize(bytes, field)
}

pub fn enforce_limit(field: &'static str, actual: u64, limit: u64) -> CatalogResult<()> {
    if actual > limit {
        return Err(CatalogError::CatalogLimitExceeded {
            field,
            actual,
            limit,
        });
    }
    Ok(())
}

pub fn align_8(value: usize) -> CatalogResult<usize> {
    value
        .checked_add(7)
        .map(|value| value & !7)
        .ok_or(CatalogError::InvalidCatalogFormat {
            detail: "alignment overflow",
        })
}

pub fn range<'a>(
    input: &'a [u8],
    offset: u64,
    length: u64,
    detail: &'static str,
) -> CatalogResult<&'a [u8]> {
    let start = checked_usize(offset, detail)?;
    let len = checked_usize(length, detail)?;
    let end = start
        .checked_add(len)
        .ok_or(CatalogError::InvalidCatalogFormat { detail })?;
    input
        .get(start..end)
        .ok_or(CatalogError::InvalidCatalogFormat { detail })
}

pub fn read_array<const N: usize>(input: &[u8], offset: usize) -> CatalogResult<[u8; N]> {
    let end = offset
        .checked_add(N)
        .ok_or(CatalogError::InvalidCatalogFormat {
            detail: "fixed-width field offset overflow",
        })?;
    input
        .get(offset..end)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(CatalogError::InvalidCatalogFormat {
            detail: "fixed-width field is out of bounds",
        })
}

pub fn read_u16(input: &[u8], offset: usize) -> CatalogResult<u16> {
    Ok(u16::from_le_bytes(read_array(input, offset)?))
}

pub fn read_u32(input: &[u8], offset: usize) -> CatalogResult<u32> {
    Ok(u32::from_le_bytes(read_array(input, offset)?))
}

pub fn read_u64(input: &[u8], offset: usize) -> CatalogResult<u64> {
    Ok(u64::from_le_bytes(read_array(input, offset)?))
}

pub fn put_bytes(output: &mut [u8], offset: usize, value: &[u8]) -> CatalogResult<()> {
    let end = offset
        .checked_add(value.len())
        .ok_or(CatalogError::InvalidCatalogFormat {
            detail: "fixed-width output field offset overflow",
        })?;
    let target = output
        .get_mut(offset..end)
        .ok_or(CatalogError::InvalidCatalogFormat {
            detail: "fixed-width output field is out of bounds",
        })?;
    target.copy_from_slice(value);
    Ok(())
}

pub fn put_u16(output: &mut [u8], offset: usize, value: u16) -> CatalogResult<()> {
    put_bytes(output, offset, &value.to_le_bytes())
}

pub fn put_u32(output: &mut [u8], offset: usize, value: u32) -> CatalogResult<()> {
    put_bytes(output, offset, &value.to_le_bytes())
}

pub fn put_u64(output: &mut [u8], offset: usize, value: u64) -> CatalogResult<()> {
    put_bytes(output, offset, &value.to_le_bytes())
}

pub fn append_aligned(output: &mut Vec<u8>) -> CatalogResult<usize> {
    let aligned = align_8(output.len())?;
    output.resize(aligned, 0);
    Ok(aligned)
}

pub fn optional_id_bytes(id: Option<crate::ObjectId>) -> [u8; 16] {
    id.map(crate::ObjectId::into_bytes).unwrap_or([0; 16])
}
