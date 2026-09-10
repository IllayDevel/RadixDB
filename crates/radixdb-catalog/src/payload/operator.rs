use crate::payload::common::{validate_flags, validate_version};
use crate::{CatalogDataType, CatalogError, CatalogResult, ObjectId};

pub const MAX_OPERATOR_LOCAL_ID_BYTES: usize = 255;
pub const MAX_OPERATOR_SYMBOL_BYTES: usize = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorPayload {
    extension_binding_id: ObjectId,
    local_id: String,
    semantic_revision: u32,
    symbol: String,
    left_argument: Option<CatalogDataType>,
    right_argument: Option<CatalogDataType>,
    result_type: CatalogDataType,
    backing_function_id: ObjectId,
}

impl OperatorPayload {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        extension_binding_id: ObjectId,
        local_id: impl Into<String>,
        semantic_revision: u32,
        symbol: impl Into<String>,
        left_argument: Option<CatalogDataType>,
        right_argument: Option<CatalogDataType>,
        result_type: CatalogDataType,
        backing_function_id: ObjectId,
    ) -> CatalogResult<Self> {
        Self::from_fields(
            super::PAYLOAD_VERSION,
            0,
            extension_binding_id,
            local_id.into(),
            semantic_revision,
            symbol.into(),
            left_argument,
            right_argument,
            result_type,
            backing_function_id,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_fields(
        version: u16,
        flags: u64,
        extension_binding_id: ObjectId,
        local_id: String,
        semantic_revision: u32,
        symbol: String,
        left_argument: Option<CatalogDataType>,
        right_argument: Option<CatalogDataType>,
        result_type: CatalogDataType,
        backing_function_id: ObjectId,
    ) -> CatalogResult<Self> {
        validate_version("operator", version)?;
        validate_flags("operator", flags)?;
        if local_id.is_empty()
            || local_id.len() > MAX_OPERATOR_LOCAL_ID_BYTES
            || local_id.contains('\0')
        {
            return Err(CatalogError::InvalidOperatorPayload {
                detail: "local id must be 1..=255 UTF-8 bytes without NUL",
            });
        }
        if semantic_revision == 0 {
            return Err(CatalogError::InvalidOperatorPayload {
                detail: "semantic revision must be at least one",
            });
        }
        if !matches!(
            symbol.as_str(),
            "=" | "<>"
                | "!="
                | "<"
                | "<="
                | ">"
                | ">="
                | "+"
                | "-"
                | "*"
                | "/"
                | "%"
                | "||"
                | "&"
                | "|"
                | "^"
                | "~"
                | "<<"
                | ">>"
                | "<=>"
                | "&&"
                | "@>"
                | "<@"
        ) {
            return Err(CatalogError::InvalidOperatorPayload {
                detail: "operator symbol is outside the closed v1 alphabet",
            });
        }
        if right_argument.is_none() || (left_argument.is_none() && right_argument.is_none()) {
            return Err(CatalogError::InvalidOperatorPayload {
                detail:
                    "operator must be unary prefix or binary; postfix operators are not admitted",
            });
        }
        Ok(Self {
            extension_binding_id,
            local_id,
            semantic_revision,
            symbol,
            left_argument,
            right_argument,
            result_type,
            backing_function_id,
        })
    }

    pub const fn flags(&self) -> u64 {
        0
    }
    pub const fn extension_binding_id(&self) -> ObjectId {
        self.extension_binding_id
    }
    pub fn local_id(&self) -> &str {
        &self.local_id
    }
    pub const fn semantic_revision(&self) -> u32 {
        self.semantic_revision
    }
    pub fn symbol(&self) -> &str {
        &self.symbol
    }
    pub const fn left_argument(&self) -> Option<CatalogDataType> {
        self.left_argument
    }
    pub const fn right_argument(&self) -> Option<CatalogDataType> {
        self.right_argument
    }
    pub const fn result_type(&self) -> CatalogDataType {
        self.result_type
    }
    pub const fn backing_function_id(&self) -> ObjectId {
        self.backing_function_id
    }
}
