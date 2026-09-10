use crate::payload::common::{ordered_unique_ids, validate_flags, validate_version, CanonicalSql};
use crate::{CatalogError, CatalogResult, ObjectId};

pub const TRIGGER_EVENT_INSERT: u16 = 1 << 0;
pub const TRIGGER_EVENT_UPDATE: u16 = 1 << 1;
pub const TRIGGER_EVENT_DELETE: u16 = 1 << 2;
pub const ALL_TRIGGER_EVENTS: u16 =
    TRIGGER_EVENT_INSERT | TRIGGER_EVENT_UPDATE | TRIGGER_EVENT_DELETE;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum TriggerTiming {
    Before = 1,
    After = 2,
}
impl TryFrom<u16> for TriggerTiming {
    type Error = CatalogError;
    fn try_from(tag: u16) -> CatalogResult<Self> {
        match tag {
            1 => Ok(Self::Before),
            2 => Ok(Self::After),
            _ => Err(CatalogError::UnknownPayloadEnumTag {
                owner: "trigger timing",
                tag,
            }),
        }
    }
}
impl TriggerTiming {
    pub const fn tag(self) -> u16 {
        self as u16
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum TriggerLevel {
    Row = 1,
    Statement = 2,
}
impl TryFrom<u16> for TriggerLevel {
    type Error = CatalogError;
    fn try_from(tag: u16) -> CatalogResult<Self> {
        match tag {
            1 => Ok(Self::Row),
            2 => Ok(Self::Statement),
            _ => Err(CatalogError::UnknownPayloadEnumTag {
                owner: "trigger level",
                tag,
            }),
        }
    }
}
impl TriggerLevel {
    pub const fn tag(self) -> u16 {
        self as u16
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerPayload {
    table_id: ObjectId,
    function_id: ObjectId,
    timing: TriggerTiming,
    events: u16,
    level: TriggerLevel,
    update_column_ids: Vec<ObjectId>,
    priority: i32,
    when_sql: Option<CanonicalSql>,
}

impl TriggerPayload {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        table_id: ObjectId,
        function_id: ObjectId,
        timing: TriggerTiming,
        events: u16,
        level: TriggerLevel,
        update_column_ids: Vec<ObjectId>,
        priority: i32,
        when_sql: Option<String>,
    ) -> CatalogResult<Self> {
        if events == 0
            || events & !ALL_TRIGGER_EVENTS != 0
            || (!update_column_ids.is_empty() && events & TRIGGER_EVENT_UPDATE == 0)
        {
            return Err(CatalogError::InvalidCatalogFormat {
                detail: "trigger event or UPDATE column set is invalid",
            });
        }
        Ok(Self {
            table_id,
            function_id,
            timing,
            events,
            level,
            update_column_ids: ordered_unique_ids(
                "trigger.update_column_ids",
                update_column_ids,
                true,
            )?,
            priority,
            when_sql: when_sql
                .map(|sql| CanonicalSql::new("trigger.when_sql", sql))
                .transpose()?,
        })
    }
    #[allow(clippy::too_many_arguments)]
    pub fn from_fields(
        version: u16,
        flags: u64,
        table_id: ObjectId,
        function_id: ObjectId,
        timing: TriggerTiming,
        events: u16,
        level: TriggerLevel,
        update_column_ids: Vec<ObjectId>,
        priority: i32,
        when_sql: Option<String>,
    ) -> CatalogResult<Self> {
        validate_version("trigger", version)?;
        validate_flags("trigger", flags)?;
        Self::new(
            table_id,
            function_id,
            timing,
            events,
            level,
            update_column_ids,
            priority,
            when_sql,
        )
    }
    pub const fn table_id(&self) -> ObjectId {
        self.table_id
    }
    pub const fn function_id(&self) -> ObjectId {
        self.function_id
    }
    pub const fn timing(&self) -> TriggerTiming {
        self.timing
    }
    pub const fn events(&self) -> u16 {
        self.events
    }
    pub const fn level(&self) -> TriggerLevel {
        self.level
    }
    pub fn update_column_ids(&self) -> &[ObjectId] {
        &self.update_column_ids
    }
    pub const fn priority(&self) -> i32 {
        self.priority
    }
    pub fn when_sql(&self) -> Option<&CanonicalSql> {
        self.when_sql.as_ref()
    }
    pub const fn flags(&self) -> u64 {
        0
    }
}
