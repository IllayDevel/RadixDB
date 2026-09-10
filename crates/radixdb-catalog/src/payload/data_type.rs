use crate::{CatalogError, CatalogResult, ObjectId};
use radixdb_core::{DataType, ExternalTypeRef, LogicalTypeRef};

const DATA_TYPE_DESCRIPTOR_VERSION: u16 = 1;
const EXTERNAL_DATA_TYPE_MARKER: u16 = 0xffff;
const EXTERNAL_DATA_TYPE_DESCRIPTOR_VERSION: u16 = 2;
pub const MAX_VECTOR_DIMENSIONS: u32 = u16::MAX as u32;

/// Validated logical form of the fixed 32-byte V6 DataTypeDescriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CatalogDataType {
    logical_type: DataType,
    descriptor_version: u16,
    parameter_1: u32,
    parameter_2: u32,
    type_object_id: [u8; 16],
}

impl CatalogDataType {
    pub fn scalar(logical_type: DataType) -> CatalogResult<Self> {
        Self::from_fields(logical_type, DATA_TYPE_DESCRIPTOR_VERSION, 0, 0, 0, [0; 16])
    }

    pub fn decimal(precision: u8, scale: u8) -> CatalogResult<Self> {
        Self::from_fields(
            DataType::Decimal,
            DATA_TYPE_DESCRIPTOR_VERSION,
            0,
            u32::from(precision),
            u32::from(scale),
            [0; 16],
        )
    }

    /// The SQL `DECIMAL`/`NUMERIC` domain without a declared typemod.
    ///
    /// Zero precision and scale are a persisted sentinel for values whose
    /// individual exact precision/scale remain part of each value payload.
    pub fn unconstrained_decimal() -> CatalogResult<Self> {
        Self::decimal(0, 0)
    }

    pub fn vector(dimensions: u16) -> CatalogResult<Self> {
        Self::from_fields(
            DataType::Vector,
            DATA_TYPE_DESCRIPTOR_VERSION,
            0,
            u32::from(dimensions),
            0,
            [0; 16],
        )
    }

    pub fn external(type_object_id: ObjectId, codec_version: u32) -> CatalogResult<Self> {
        if codec_version == 0 {
            return Err(CatalogError::InvalidDataTypeDescriptor {
                detail: "external codec version must be at least one",
            });
        }
        Ok(Self {
            logical_type: DataType::Null,
            descriptor_version: EXTERNAL_DATA_TYPE_DESCRIPTOR_VERSION,
            parameter_1: codec_version,
            parameter_2: 0,
            type_object_id: type_object_id.into_bytes(),
        })
    }

    pub fn from_fields(
        logical_type: DataType,
        descriptor_version: u16,
        flags: u32,
        parameter_1: u32,
        parameter_2: u32,
        collation_id: [u8; 16],
    ) -> CatalogResult<Self> {
        if descriptor_version != DATA_TYPE_DESCRIPTOR_VERSION {
            return Err(CatalogError::InvalidDataTypeDescriptor {
                detail: "unsupported descriptor version",
            });
        }
        if flags != 0 {
            return Err(CatalogError::InvalidDataTypeDescriptor {
                detail: "unknown descriptor flags",
            });
        }
        if collation_id != [0; 16] {
            return Err(CatalogError::InvalidDataTypeDescriptor {
                detail: "non-binary collation is not admitted in V6.0",
            });
        }

        match logical_type {
            DataType::Null => {
                return Err(CatalogError::InvalidDataTypeDescriptor {
                    detail: "NULL is not a stored column type",
                });
            }
            DataType::Decimal
                if !((parameter_1 == 0 && parameter_2 == 0)
                    || ((1..=38).contains(&parameter_1) && parameter_2 <= parameter_1)) =>
            {
                return Err(CatalogError::InvalidDataTypeDescriptor {
                    detail: "decimal precision/scale is neither unconstrained nor inside 1..=38",
                });
            }
            DataType::Vector if parameter_1 == 0 || parameter_1 > MAX_VECTOR_DIMENSIONS => {
                return Err(CatalogError::InvalidDataTypeDescriptor {
                    detail: "vector dimensions are outside 1..=65535",
                });
            }
            DataType::Decimal | DataType::Vector => {}
            _ if parameter_1 != 0 || parameter_2 != 0 => {
                return Err(CatalogError::InvalidDataTypeDescriptor {
                    detail: "scalar type parameters must be zero",
                });
            }
            _ => {}
        }
        if logical_type == DataType::Vector && parameter_2 != 0 {
            return Err(CatalogError::InvalidDataTypeDescriptor {
                detail: "vector parameter_2 must be zero",
            });
        }

        Ok(Self {
            logical_type,
            descriptor_version,
            parameter_1,
            parameter_2,
            type_object_id: collation_id,
        })
    }

    pub const fn logical_type(self) -> DataType {
        self.logical_type
    }

    pub const fn descriptor_version(self) -> u16 {
        self.descriptor_version
    }

    pub const fn descriptor_marker(self) -> u16 {
        if self.is_external() {
            EXTERNAL_DATA_TYPE_MARKER
        } else {
            self.logical_type as u16
        }
    }

    pub const fn parameter_1(self) -> u32 {
        self.parameter_1
    }

    pub const fn parameter_2(self) -> u32 {
        self.parameter_2
    }

    pub const fn flags(self) -> u32 {
        0
    }

    pub const fn collation_id(self) -> [u8; 16] {
        self.type_object_id
    }

    pub const fn is_external(self) -> bool {
        self.descriptor_version == EXTERNAL_DATA_TYPE_DESCRIPTOR_VERSION
    }

    pub fn external_type_ref(self) -> Option<ExternalTypeRef> {
        self.is_external()
            .then(|| ExternalTypeRef::new(self.type_object_id, self.parameter_1).ok())
            .flatten()
    }

    pub fn logical_type_ref(self) -> LogicalTypeRef {
        self.external_type_ref()
            .map(LogicalTypeRef::External)
            .unwrap_or(LogicalTypeRef::Builtin(self.logical_type))
    }

    pub fn type_object_id(self) -> Option<ObjectId> {
        self.is_external()
            .then(|| ObjectId::from_bytes(self.type_object_id).ok())
            .flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_semantically_valid_parameters_are_admitted() {
        assert!(CatalogDataType::scalar(DataType::Integer).is_ok());
        assert!(CatalogDataType::scalar(DataType::Null).is_err());
        assert!(CatalogDataType::unconstrained_decimal().is_ok());
        assert!(CatalogDataType::decimal(38, 38).is_ok());
        assert!(CatalogDataType::decimal(0, 1).is_err());
        assert!(CatalogDataType::decimal(2, 3).is_err());
        assert!(CatalogDataType::vector(u16::MAX).is_ok());
        assert!(CatalogDataType::from_fields(DataType::Text, 1, 0, 1, 0, [0; 16]).is_err());
        assert!(CatalogDataType::from_fields(DataType::Text, 2, 0, 0, 0, [0; 16]).is_err());
        assert!(CatalogDataType::from_fields(DataType::Text, 1, 1, 0, 0, [0; 16]).is_err());
        assert!(CatalogDataType::from_fields(DataType::Text, 1, 0, 0, 0, [1; 16]).is_err());
        let external =
            CatalogDataType::external(ObjectId::from_bytes([7; 16]).unwrap(), 3).unwrap();
        assert!(external.is_external());
        assert_eq!(external.descriptor_marker(), 0xffff);
        assert_eq!(external.descriptor_version(), 2);
        assert_eq!(external.parameter_1(), 3);
        assert_eq!(external.type_object_id().unwrap().into_bytes(), [7; 16]);
    }
}
