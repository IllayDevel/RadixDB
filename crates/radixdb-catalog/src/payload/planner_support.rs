use crate::payload::common::{validate_flags, validate_version};
use crate::{CatalogError, CatalogResult, ObjectId};

pub const MAX_PLANNER_SUPPORT_LOCAL_ID_BYTES: usize = 255;
pub const MAX_PLANNER_SUPPORT_SPANS: u32 = 4096;
pub const MAX_PLANNER_SUPPORT_OUTPUT_BYTES: u32 = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum PlannerRecheckPolicy {
    Exact = 1,
    Always = 2,
}

impl PlannerRecheckPolicy {
    pub const fn tag(self) -> u16 {
        self as u16
    }
}

impl TryFrom<u16> for PlannerRecheckPolicy {
    type Error = CatalogError;

    fn try_from(tag: u16) -> CatalogResult<Self> {
        match tag {
            1 => Ok(Self::Exact),
            2 => Ok(Self::Always),
            _ => Err(CatalogError::UnknownPayloadEnumTag {
                owner: "planner support recheck policy",
                tag,
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannerSupportPayload {
    extension_binding_id: ObjectId,
    local_id: String,
    semantic_revision: u32,
    target_function_id: Option<ObjectId>,
    target_operator_class_id: Option<ObjectId>,
    max_spans: u32,
    max_output_bytes: u32,
    recheck_policy: PlannerRecheckPolicy,
    fingerprint: [u8; 32],
}

impl PlannerSupportPayload {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        extension_binding_id: ObjectId,
        local_id: impl Into<String>,
        semantic_revision: u32,
        target_function_id: Option<ObjectId>,
        target_operator_class_id: Option<ObjectId>,
        max_spans: u32,
        max_output_bytes: u32,
        recheck_policy: PlannerRecheckPolicy,
        fingerprint: [u8; 32],
    ) -> CatalogResult<Self> {
        Self::from_fields(
            super::PAYLOAD_VERSION,
            0,
            extension_binding_id,
            local_id.into(),
            semantic_revision,
            target_function_id,
            target_operator_class_id,
            max_spans,
            max_output_bytes,
            recheck_policy,
            fingerprint,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_fields(
        version: u16,
        flags: u64,
        extension_binding_id: ObjectId,
        local_id: String,
        semantic_revision: u32,
        target_function_id: Option<ObjectId>,
        target_operator_class_id: Option<ObjectId>,
        max_spans: u32,
        max_output_bytes: u32,
        recheck_policy: PlannerRecheckPolicy,
        fingerprint: [u8; 32],
    ) -> CatalogResult<Self> {
        validate_version("planner support", version)?;
        validate_flags("planner support", flags)?;
        if local_id.is_empty()
            || local_id.len() > MAX_PLANNER_SUPPORT_LOCAL_ID_BYTES
            || local_id.contains('\0')
        {
            return Err(CatalogError::InvalidPlannerSupportPayload {
                detail: "local id must be 1..=255 UTF-8 bytes without NUL",
            });
        }
        if semantic_revision == 0 {
            return Err(CatalogError::InvalidPlannerSupportPayload {
                detail: "semantic revision must be at least one",
            });
        }
        if target_function_id.is_none() && target_operator_class_id.is_none() {
            return Err(CatalogError::InvalidPlannerSupportPayload {
                detail: "at least one target is required",
            });
        }
        if !(1..=MAX_PLANNER_SUPPORT_SPANS).contains(&max_spans) {
            return Err(CatalogError::InvalidPlannerSupportPayload {
                detail: "maximum spans must be in 1..=4096",
            });
        }
        if !(1..=MAX_PLANNER_SUPPORT_OUTPUT_BYTES).contains(&max_output_bytes) {
            return Err(CatalogError::InvalidPlannerSupportPayload {
                detail: "maximum output bytes must be in 1..=16 MiB",
            });
        }
        Ok(Self {
            extension_binding_id,
            local_id,
            semantic_revision,
            target_function_id,
            target_operator_class_id,
            max_spans,
            max_output_bytes,
            recheck_policy,
            fingerprint,
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
    pub const fn target_function_id(&self) -> Option<ObjectId> {
        self.target_function_id
    }
    pub const fn target_operator_class_id(&self) -> Option<ObjectId> {
        self.target_operator_class_id
    }
    pub const fn max_spans(&self) -> u32 {
        self.max_spans
    }
    pub const fn max_output_bytes(&self) -> u32 {
        self.max_output_bytes
    }
    pub const fn recheck_policy(&self) -> PlannerRecheckPolicy {
        self.recheck_policy
    }
    pub const fn fingerprint(&self) -> &[u8; 32] {
        &self.fingerprint
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(byte: u8) -> ObjectId {
        ObjectId::from_user_bytes([byte; 16]).unwrap()
    }

    #[test]
    fn planner_support_contract_is_exact() {
        let payload = PlannerSupportPayload::new(
            id(1),
            "point_eq_support",
            3,
            Some(id(2)),
            Some(id(3)),
            64,
            4096,
            PlannerRecheckPolicy::Always,
            [4; 32],
        )
        .unwrap();
        assert_eq!(payload.extension_binding_id(), id(1));
        assert_eq!(payload.target_function_id(), Some(id(2)));
        assert_eq!(payload.target_operator_class_id(), Some(id(3)));
        assert_eq!(payload.max_spans(), 64);
        assert_eq!(payload.max_output_bytes(), 4096);
        assert_eq!(payload.recheck_policy(), PlannerRecheckPolicy::Always);
        assert_eq!(
            PlannerRecheckPolicy::try_from(1).unwrap(),
            PlannerRecheckPolicy::Exact
        );
        assert!(PlannerRecheckPolicy::try_from(3).is_err());
    }

    #[test]
    fn planner_support_bounds_fail_closed() {
        let make = |target_function, target_class, spans, bytes| {
            PlannerSupportPayload::new(
                id(1),
                "support",
                1,
                target_function,
                target_class,
                spans,
                bytes,
                PlannerRecheckPolicy::Exact,
                [0; 32],
            )
        };
        assert!(make(None, None, 1, 1).is_err());
        assert!(make(Some(id(2)), None, 0, 1).is_err());
        assert!(make(Some(id(2)), None, MAX_PLANNER_SUPPORT_SPANS + 1, 1).is_err());
        assert!(make(Some(id(2)), None, 1, 0).is_err());
        assert!(make(Some(id(2)), None, 1, MAX_PLANNER_SUPPORT_OUTPUT_BYTES + 1,).is_err());
    }
}
