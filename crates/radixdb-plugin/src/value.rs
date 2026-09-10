use std::cmp::Ordering;

use radixdb_plugin_abi::{
    RadixAbiCodecFnV1, RadixAbiCompareFnV1, RadixAbiEqualFnV1, RadixAbiHashFnV1, RadixAbiParseFnV1,
    RadixAbiTypeRefV1, RadixAbiValueV1, RADIX_BUILTIN_BOOLEAN, RADIX_BUILTIN_BYTES,
    RADIX_BUILTIN_FLOAT, RADIX_BUILTIN_INTEGER, RADIX_BUILTIN_TEXT, RADIX_TYPE_REF_EXTERNAL,
    RADIX_VALUE_FLAG_NULL,
};

use crate::{CodecReader, CodecWriter, HashSink, PluginError, PluginResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundedBytes<const MAX: usize>(Vec<u8>);

impl<const MAX: usize> BoundedBytes<MAX> {
    pub fn new(bytes: impl Into<Vec<u8>>) -> PluginResult<Self> {
        let bytes = bytes.into();
        if bytes.len() > MAX {
            return Err(PluginError::limit_exceeded(
                "byte value exceeds its declared bound",
            ));
        }
        Ok(Self(bytes))
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundedText<const MAX: usize>(String);

impl<const MAX: usize> BoundedText<MAX> {
    pub fn new(text: impl Into<String>) -> PluginResult<Self> {
        let text = text.into();
        if text.len() > MAX {
            return Err(PluginError::limit_exceeded(
                "text value exceeds its declared bound",
            ));
        }
        Ok(Self(text))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

pub trait RadixType: Sized + Send + Sync + 'static {
    const LOCAL_ID: &'static str;
    const DISPLAY_NAME: &'static str;
    const CODEC_VERSION: u32;
    const SEMANTIC_REVISION: u32;
    const STORAGE_KIND: u16;
    const FIXED_BYTES: u32;
    const MAX_BYTES: u32;
    const CAPABILITIES: u64;
    const CODEC_FINGERPRINT: [u8; 32];

    #[doc(hidden)]
    const ABI_ENCODE: Option<RadixAbiCodecFnV1>;
    #[doc(hidden)]
    const ABI_DECODE: Option<RadixAbiParseFnV1>;
    #[doc(hidden)]
    const ABI_EQUALITY: Option<RadixAbiEqualFnV1>;
    #[doc(hidden)]
    const ABI_HASH: Option<RadixAbiHashFnV1>;
    #[doc(hidden)]
    const ABI_ORDERING: Option<RadixAbiCompareFnV1>;

    fn encode(&self, output: &mut CodecWriter) -> PluginResult<()>;
    fn decode(input: &mut CodecReader<'_>) -> PluginResult<Self>;
    fn test_corpus() -> Vec<Self>;

    fn semantic_equal(_left: &Self, _right: &Self) -> Option<bool> {
        None
    }

    fn semantic_hash(_value: &Self, _sink: &mut HashSink<'_>) -> Option<PluginResult<()>> {
        None
    }

    fn semantic_compare(_left: &Self, _right: &Self) -> Option<Ordering> {
        None
    }
}

pub trait ValueType: Sized {
    const TYPE_REF: RadixAbiTypeRefV1;
    const MAX_OUTPUT_BYTES: u32;

    #[doc(hidden)]
    fn decode_abi(value: &AbiValue<'_>) -> PluginResult<Self>;
    #[doc(hidden)]
    fn encode_abi(&self) -> PluginResult<Vec<u8>>;

    #[doc(hidden)]
    fn is_null(&self) -> bool {
        false
    }
}

#[doc(hidden)]
pub struct AbiValue<'a> {
    raw: &'a RadixAbiValueV1,
    bytes: &'a [u8],
}

impl<'a> AbiValue<'a> {
    pub(crate) unsafe fn new(raw: &'a RadixAbiValueV1) -> PluginResult<Self> {
        if raw.reserved != 0 || raw.flags & !RADIX_VALUE_FLAG_NULL != 0 {
            return Err(PluginError::invalid_input("invalid ABI value flags"));
        }
        let bytes = if raw.borrowed_bytes.len == 0 {
            &[]
        } else {
            if raw.borrowed_bytes.ptr.is_null() || raw.borrowed_bytes.reserved != 0 {
                return Err(PluginError::invalid_input("invalid ABI value slice"));
            }
            // SAFETY: the host owns this call-scoped range and the ABI contract
            // requires it to remain readable for the duration of the callback.
            unsafe {
                std::slice::from_raw_parts(raw.borrowed_bytes.ptr, raw.borrowed_bytes.len as usize)
            }
        };
        Ok(Self { raw, bytes })
    }

    pub fn is_null(&self) -> bool {
        self.raw.flags & RADIX_VALUE_FLAG_NULL != 0
    }

    fn expect_type(&self, expected: RadixAbiTypeRefV1) -> PluginResult<()> {
        if self.raw.type_ref != expected {
            return Err(PluginError::invalid_input("ABI value type mismatch"));
        }
        Ok(())
    }
}

macro_rules! integer_value {
    ($type:ty) => {
        impl ValueType for $type {
            const TYPE_REF: RadixAbiTypeRefV1 = RadixAbiTypeRefV1::builtin(RADIX_BUILTIN_INTEGER);
            const MAX_OUTPUT_BYTES: u32 = 8;

            fn decode_abi(value: &AbiValue<'_>) -> PluginResult<Self> {
                value.expect_type(Self::TYPE_REF)?;
                if value.is_null() {
                    return Err(PluginError::invalid_input("unexpected NULL argument"));
                }
                let raw = i64::from_le_bytes(value.raw.inline_bytes[..8].try_into().unwrap());
                <$type>::try_from(raw)
                    .map_err(|_| PluginError::invalid_input("integer argument is out of range"))
            }

            fn encode_abi(&self) -> PluginResult<Vec<u8>> {
                let value = i64::try_from(*self)
                    .map_err(|_| PluginError::domain("integer result is out of ABI range"))?;
                Ok(value.to_le_bytes().to_vec())
            }
        }
    };
}

integer_value!(i8);
integer_value!(i16);
integer_value!(i32);
integer_value!(i64);
integer_value!(u8);
integer_value!(u16);
integer_value!(u32);

impl ValueType for u64 {
    const TYPE_REF: RadixAbiTypeRefV1 = RadixAbiTypeRefV1::builtin(RADIX_BUILTIN_INTEGER);
    const MAX_OUTPUT_BYTES: u32 = 8;

    fn decode_abi(value: &AbiValue<'_>) -> PluginResult<Self> {
        value.expect_type(Self::TYPE_REF)?;
        if value.is_null() {
            return Err(PluginError::invalid_input("unexpected NULL argument"));
        }
        let raw = i64::from_le_bytes(value.raw.inline_bytes[..8].try_into().unwrap());
        Self::try_from(raw).map_err(|_| PluginError::invalid_input("negative integer for u64"))
    }

    fn encode_abi(&self) -> PluginResult<Vec<u8>> {
        let value = i64::try_from(*self)
            .map_err(|_| PluginError::domain("u64 result exceeds signed ABI integer"))?;
        Ok(value.to_le_bytes().to_vec())
    }
}

impl ValueType for f64 {
    const TYPE_REF: RadixAbiTypeRefV1 = RadixAbiTypeRefV1::builtin(RADIX_BUILTIN_FLOAT);
    const MAX_OUTPUT_BYTES: u32 = 8;

    fn decode_abi(value: &AbiValue<'_>) -> PluginResult<Self> {
        value.expect_type(Self::TYPE_REF)?;
        if value.is_null() {
            return Err(PluginError::invalid_input("unexpected NULL argument"));
        }
        Ok(Self::from_bits(u64::from_le_bytes(
            value.raw.inline_bytes[..8].try_into().unwrap(),
        )))
    }

    fn encode_abi(&self) -> PluginResult<Vec<u8>> {
        Ok(self.to_bits().to_le_bytes().to_vec())
    }
}

impl ValueType for f32 {
    const TYPE_REF: RadixAbiTypeRefV1 = RadixAbiTypeRefV1::builtin(RADIX_BUILTIN_FLOAT);
    const MAX_OUTPUT_BYTES: u32 = 8;

    fn decode_abi(value: &AbiValue<'_>) -> PluginResult<Self> {
        Ok(f64::decode_abi(value)? as f32)
    }

    fn encode_abi(&self) -> PluginResult<Vec<u8>> {
        (*self as f64).encode_abi()
    }
}

impl ValueType for bool {
    const TYPE_REF: RadixAbiTypeRefV1 = RadixAbiTypeRefV1::builtin(RADIX_BUILTIN_BOOLEAN);
    const MAX_OUTPUT_BYTES: u32 = 1;

    fn decode_abi(value: &AbiValue<'_>) -> PluginResult<Self> {
        value.expect_type(Self::TYPE_REF)?;
        if value.is_null() {
            return Err(PluginError::invalid_input("unexpected NULL argument"));
        }
        match value.raw.inline_bytes[0] {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(PluginError::invalid_input("invalid ABI boolean")),
        }
    }

    fn encode_abi(&self) -> PluginResult<Vec<u8>> {
        Ok(vec![u8::from(*self)])
    }
}

impl<const MAX: usize> ValueType for BoundedBytes<MAX> {
    const TYPE_REF: RadixAbiTypeRefV1 = RadixAbiTypeRefV1::builtin(RADIX_BUILTIN_BYTES);
    const MAX_OUTPUT_BYTES: u32 = if MAX > u32::MAX as usize {
        u32::MAX
    } else {
        MAX as u32
    };

    fn decode_abi(value: &AbiValue<'_>) -> PluginResult<Self> {
        value.expect_type(Self::TYPE_REF)?;
        if value.is_null() {
            return Err(PluginError::invalid_input("unexpected NULL argument"));
        }
        Self::new(value.bytes)
    }

    fn encode_abi(&self) -> PluginResult<Vec<u8>> {
        Ok(self.0.clone())
    }
}

impl<const MAX: usize> ValueType for BoundedText<MAX> {
    const TYPE_REF: RadixAbiTypeRefV1 = RadixAbiTypeRefV1::builtin(RADIX_BUILTIN_TEXT);
    const MAX_OUTPUT_BYTES: u32 = if MAX > u32::MAX as usize {
        u32::MAX
    } else {
        MAX as u32
    };

    fn decode_abi(value: &AbiValue<'_>) -> PluginResult<Self> {
        value.expect_type(Self::TYPE_REF)?;
        if value.is_null() {
            return Err(PluginError::invalid_input("unexpected NULL argument"));
        }
        let text = std::str::from_utf8(value.bytes)
            .map_err(|_| PluginError::invalid_input("text argument is not UTF-8"))?;
        Self::new(text)
    }

    fn encode_abi(&self) -> PluginResult<Vec<u8>> {
        Ok(self.0.as_bytes().to_vec())
    }
}

impl<T: RadixType> ValueType for T {
    const TYPE_REF: RadixAbiTypeRefV1 = RadixAbiTypeRefV1 {
        kind: RADIX_TYPE_REF_EXTERNAL,
        builtin_tag: 0,
        codec_version: T::CODEC_VERSION,
        object_id: [0; 16],
    };
    const MAX_OUTPUT_BYTES: u32 = T::MAX_BYTES;

    fn decode_abi(value: &AbiValue<'_>) -> PluginResult<Self> {
        if value.raw.type_ref.kind != RADIX_TYPE_REF_EXTERNAL
            || value.raw.type_ref.codec_version != T::CODEC_VERSION
            || value.is_null()
        {
            return Err(PluginError::invalid_input(
                "external ABI value type or codec mismatch",
            ));
        }
        if T::STORAGE_KIND == radixdb_plugin_abi::RADIX_EXTERNAL_STORAGE_FIXED
            && value.bytes.len() != T::FIXED_BYTES as usize
        {
            return Err(PluginError::invalid_input(
                "fixed external value has wrong encoded width",
            ));
        }
        let mut reader = CodecReader::new(value.bytes);
        let decoded = T::decode(&mut reader)?;
        reader.finish()?;
        Ok(decoded)
    }

    fn encode_abi(&self) -> PluginResult<Vec<u8>> {
        let mut output = CodecWriter::new(T::MAX_BYTES as usize);
        self.encode(&mut output)?;
        let bytes = output.into_bytes();
        if T::STORAGE_KIND == radixdb_plugin_abi::RADIX_EXTERNAL_STORAGE_FIXED
            && bytes.len() != T::FIXED_BYTES as usize
        {
            return Err(PluginError::internal(
                "fixed external codec emitted the wrong width",
            ));
        }
        Ok(bytes)
    }
}

impl<T: ValueType> ValueType for Option<T> {
    const TYPE_REF: RadixAbiTypeRefV1 = T::TYPE_REF;
    const MAX_OUTPUT_BYTES: u32 = T::MAX_OUTPUT_BYTES;

    fn decode_abi(value: &AbiValue<'_>) -> PluginResult<Self> {
        if value.is_null() {
            Ok(None)
        } else {
            T::decode_abi(value).map(Some)
        }
    }

    fn encode_abi(&self) -> PluginResult<Vec<u8>> {
        match self {
            Some(value) => value.encode_abi(),
            None => Ok(Vec::new()),
        }
    }

    fn is_null(&self) -> bool {
        self.is_none()
    }
}
