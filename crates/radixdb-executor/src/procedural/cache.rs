use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use radixdb_catalog::ObjectId;

use super::trigger::PublishedTrigger;
use super::PublishedRoutine;

const MAX_COMPILED_ROUTINES: usize = 1024;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct RoutineCacheKey {
    pub database_id: [u8; 16],
    pub catalog_id: [u8; 16],
    pub catalog_generation: u64,
    pub object_id: ObjectId,
    pub definition_revision: u64,
    pub source_digest: [u8; 32],
    pub dependency_versions: Vec<(ObjectId, u64)>,
    pub compiler_abi: u32,
    pub runtime_abi: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct TriggerCacheKey {
    pub database_id: [u8; 16],
    pub catalog_id: [u8; 16],
    pub catalog_generation: u64,
    pub trigger_id: ObjectId,
    pub trigger_revision: u64,
    pub function_id: ObjectId,
    pub function_revision: u64,
    pub source_digest: [u8; 32],
    pub table_id: ObjectId,
    pub table_revision: u64,
    pub event: u16,
    pub timing: u8,
    pub level: u8,
    pub dependency_versions: Vec<(ObjectId, u64)>,
    pub compiler_abi: u32,
    pub runtime_abi: u32,
}

#[derive(Default)]
pub(crate) struct ProceduralProgramCache {
    entries: Mutex<BTreeMap<RoutineCacheKey, Arc<PublishedRoutine>>>,
    trigger_entries: Mutex<BTreeMap<TriggerCacheKey, Arc<PublishedTrigger>>>,
}

impl ProceduralProgramCache {
    pub(super) fn get(&self, key: &RoutineCacheKey) -> Option<Arc<PublishedRoutine>> {
        self.entries.lock().unwrap().get(key).cloned()
    }

    pub(super) fn insert(
        &self,
        key: RoutineCacheKey,
        routine: Arc<PublishedRoutine>,
    ) -> Arc<PublishedRoutine> {
        let mut entries = self.entries.lock().unwrap();
        if entries.len() >= MAX_COMPILED_ROUTINES {
            entries.clear();
        }
        entries.insert(key, Arc::clone(&routine));
        routine
    }

    pub(crate) fn clear(&self) {
        self.entries.lock().unwrap().clear();
        self.trigger_entries.lock().unwrap().clear();
    }

    pub(super) fn get_trigger(&self, key: &TriggerCacheKey) -> Option<Arc<PublishedTrigger>> {
        self.trigger_entries.lock().unwrap().get(key).cloned()
    }

    pub(super) fn insert_trigger(
        &self,
        key: TriggerCacheKey,
        trigger: Arc<PublishedTrigger>,
    ) -> Arc<PublishedTrigger> {
        let mut entries = self.trigger_entries.lock().unwrap();
        if entries.len() >= MAX_COMPILED_ROUTINES {
            entries.clear();
        }
        entries.insert(key, Arc::clone(&trigger));
        trigger
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.lock().unwrap().len() + self.trigger_entries.lock().unwrap().len()
    }
}
