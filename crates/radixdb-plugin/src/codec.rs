use crate::{PluginError, PluginResult};

#[derive(Debug)]
pub struct CodecWriter {
    bytes: Vec<u8>,
    max_bytes: usize,
}

impl CodecWriter {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            bytes: Vec::new(),
            max_bytes,
        }
    }

    pub fn write(&mut self, bytes: &[u8]) -> PluginResult<()> {
        let next = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .ok_or_else(|| PluginError::limit_exceeded("codec output size overflow"))?;
        if next > self.max_bytes {
            return Err(PluginError::limit_exceeded(
                "codec output exceeds declared max_bytes",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CodecReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> CodecReader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    pub fn read(&mut self, len: usize) -> PluginResult<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| PluginError::invalid_input("codec input offset overflow"))?;
        let result = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| PluginError::invalid_input("truncated canonical value"))?;
        self.offset = end;
        Ok(result)
    }

    pub fn finish(self) -> PluginResult<()> {
        if self.offset != self.bytes.len() {
            return Err(PluginError::invalid_input(
                "canonical value has trailing bytes",
            ));
        }
        Ok(())
    }
}

pub trait ManualCodec<T>: Send + Sync + 'static {
    fn encode(value: &T, output: &mut CodecWriter) -> PluginResult<()>;
    fn decode(input: &mut CodecReader<'_>) -> PluginResult<T>;
    fn corpus() -> Vec<T>;
}

#[doc(hidden)]
pub trait CanonicalField: Sized + Clone {
    fn encode_field(&self, output: &mut CodecWriter) -> PluginResult<()>;
    fn decode_field(input: &mut CodecReader<'_>) -> PluginResult<Self>;
    fn edge_values() -> Vec<Self>;
}

macro_rules! integer_field {
    ($type:ty) => {
        impl CanonicalField for $type {
            fn encode_field(&self, output: &mut CodecWriter) -> PluginResult<()> {
                output.write(&self.to_le_bytes())
            }

            fn decode_field(input: &mut CodecReader<'_>) -> PluginResult<Self> {
                let bytes: [u8; std::mem::size_of::<Self>()] = input
                    .read(std::mem::size_of::<Self>())?
                    .try_into()
                    .expect("fixed-width slice length was checked");
                Ok(Self::from_le_bytes(bytes))
            }

            fn edge_values() -> Vec<Self> {
                vec![Self::MIN, 0, Self::MAX]
            }
        }
    };
}

integer_field!(i8);
integer_field!(i16);
integer_field!(i32);
integer_field!(i64);
integer_field!(u8);
integer_field!(u16);
integer_field!(u32);
integer_field!(u64);

impl CanonicalField for bool {
    fn encode_field(&self, output: &mut CodecWriter) -> PluginResult<()> {
        output.write(&[u8::from(*self)])
    }

    fn decode_field(input: &mut CodecReader<'_>) -> PluginResult<Self> {
        match input.read(1)?[0] {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(PluginError::invalid_input(
                "boolean field must be encoded as 0 or 1",
            )),
        }
    }

    fn edge_values() -> Vec<Self> {
        vec![false, true]
    }
}

impl CanonicalField for f32 {
    fn encode_field(&self, output: &mut CodecWriter) -> PluginResult<()> {
        output.write(&self.to_bits().to_le_bytes())
    }

    fn decode_field(input: &mut CodecReader<'_>) -> PluginResult<Self> {
        let bytes = input.read(4)?.try_into().expect("fixed width");
        Ok(Self::from_bits(u32::from_le_bytes(bytes)))
    }

    fn edge_values() -> Vec<Self> {
        vec![
            Self::NEG_INFINITY,
            -0.0,
            0.0,
            Self::INFINITY,
            Self::from_bits(0x7fc0_0001),
        ]
    }
}

impl CanonicalField for f64 {
    fn encode_field(&self, output: &mut CodecWriter) -> PluginResult<()> {
        output.write(&self.to_bits().to_le_bytes())
    }

    fn decode_field(input: &mut CodecReader<'_>) -> PluginResult<Self> {
        let bytes = input.read(8)?.try_into().expect("fixed width");
        Ok(Self::from_bits(u64::from_le_bytes(bytes)))
    }

    fn edge_values() -> Vec<Self> {
        vec![
            Self::NEG_INFINITY,
            -0.0,
            0.0,
            Self::INFINITY,
            Self::from_bits(0x7ff8_0000_0000_0001),
        ]
    }
}

impl<T: CanonicalField, const N: usize> CanonicalField for [T; N] {
    fn encode_field(&self, output: &mut CodecWriter) -> PluginResult<()> {
        for value in self {
            value.encode_field(output)?;
        }
        Ok(())
    }

    fn decode_field(input: &mut CodecReader<'_>) -> PluginResult<Self> {
        let mut values = Vec::with_capacity(N);
        for _ in 0..N {
            values.push(T::decode_field(input)?);
        }
        values
            .try_into()
            .map_err(|_| PluginError::internal("fixed array decoder length mismatch"))
    }

    fn edge_values() -> Vec<Self> {
        T::edge_values()
            .into_iter()
            .map(|value| std::array::from_fn(|_| value.clone()))
            .collect()
    }
}

#[doc(hidden)]
pub fn encode_sequence<T: CanonicalField>(
    values: &[T],
    max_items: usize,
    max_bytes: usize,
    output: &mut CodecWriter,
) -> PluginResult<()> {
    if values.len() > max_items || values.len() > u32::MAX as usize {
        return Err(PluginError::limit_exceeded(
            "sequence exceeds declared max_items",
        ));
    }
    let before = output.len();
    output.write(&(values.len() as u32).to_le_bytes())?;
    if output.len() - before > max_bytes {
        return Err(PluginError::limit_exceeded(
            "sequence exceeds declared max_bytes",
        ));
    }
    for value in values {
        value.encode_field(output)?;
        if output.len() - before > max_bytes {
            return Err(PluginError::limit_exceeded(
                "sequence exceeds declared max_bytes",
            ));
        }
    }
    Ok(())
}

#[doc(hidden)]
pub fn decode_sequence<T: CanonicalField>(
    input: &mut CodecReader<'_>,
    max_items: usize,
    max_bytes: usize,
) -> PluginResult<Vec<T>> {
    let count = u32::from_le_bytes(input.read(4)?.try_into().expect("fixed width")) as usize;
    if count > max_items {
        return Err(PluginError::limit_exceeded(
            "sequence exceeds declared max_items",
        ));
    }
    let mut output = CodecWriter::new(max_bytes);
    output.write(&(count as u32).to_le_bytes())?;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let value = T::decode_field(input)?;
        value.encode_field(&mut output)?;
        values.push(value);
    }
    Ok(values)
}
