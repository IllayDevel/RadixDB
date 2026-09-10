use super::model::{
    HeartbeatSnapshot, PlannedSilence, SemanticProgressSnapshot, DIAGNOSTIC_FORMAT_V2,
};

pub struct SemanticProgressTracker {
    heartbeats: HeartbeatSnapshot,
    progress: SemanticProgressSnapshot,
}

impl SemanticProgressTracker {
    pub fn new(unix_millis: u64, phase: impl Into<String>) -> Result<Self, String> {
        let progress = SemanticProgressSnapshot {
            format: DIAGNOSTIC_FORMAT_V2,
            sequence: 1,
            phase_epoch: 1,
            phase: phase.into(),
            phase_started_unix_millis: unix_millis,
            workload_epoch: 0,
            workload_units: 0,
            last_workload_progress_unix_millis: unix_millis,
            operation_epoch: 0,
            active_operation: None,
            last_operation_progress_unix_millis: unix_millis,
            planned_silence: None,
        };
        progress.validate()?;
        Ok(Self {
            heartbeats: HeartbeatSnapshot {
                agent_unix_millis: Some(unix_millis),
                ..HeartbeatSnapshot::default()
            },
            progress,
        })
    }

    pub fn heartbeat(&mut self, unix_millis: u64) {
        self.heartbeats.agent_unix_millis = Some(unix_millis);
    }

    pub fn advance_phase(
        &mut self,
        unix_millis: u64,
        phase: impl Into<String>,
    ) -> Result<&SemanticProgressSnapshot, String> {
        self.bump_sequence()?;
        self.progress.phase_epoch = self
            .progress
            .phase_epoch
            .checked_add(1)
            .ok_or_else(|| "semantic phase epoch exhausted".to_string())?;
        self.progress.phase = phase.into();
        self.progress.phase_started_unix_millis = unix_millis;
        self.progress.last_workload_progress_unix_millis = unix_millis;
        self.progress.last_operation_progress_unix_millis = unix_millis;
        self.progress.active_operation = None;
        self.progress.planned_silence = None;
        self.heartbeat(unix_millis);
        self.progress.validate()?;
        Ok(&self.progress)
    }

    pub fn advance_workload(
        &mut self,
        unix_millis: u64,
        units: u64,
    ) -> Result<Option<&SemanticProgressSnapshot>, String> {
        self.heartbeat(unix_millis);
        if units == 0 {
            return Ok(None);
        }
        self.bump_sequence()?;
        self.progress.workload_epoch = self
            .progress
            .workload_epoch
            .checked_add(1)
            .ok_or_else(|| "semantic workload epoch exhausted".to_string())?;
        self.progress.workload_units = self.progress.workload_units.saturating_add(units);
        self.progress.last_workload_progress_unix_millis = unix_millis;
        self.progress.validate()?;
        Ok(Some(&self.progress))
    }

    pub fn begin_operation(
        &mut self,
        unix_millis: u64,
        operation: impl Into<String>,
        planned_silence_deadline: Option<u64>,
    ) -> Result<&SemanticProgressSnapshot, String> {
        self.bump_sequence()?;
        self.progress.operation_epoch = self
            .progress
            .operation_epoch
            .checked_add(1)
            .ok_or_else(|| "semantic operation epoch exhausted".to_string())?;
        let operation = operation.into();
        self.progress.active_operation = Some(operation.clone());
        self.progress.last_operation_progress_unix_millis = unix_millis;
        self.progress.planned_silence = planned_silence_deadline.map(|deadline| PlannedSilence {
            reason: operation,
            started_unix_millis: unix_millis,
            deadline_unix_millis: deadline,
            phase_epoch: self.progress.phase_epoch,
        });
        self.heartbeat(unix_millis);
        self.progress.validate()?;
        Ok(&self.progress)
    }

    pub fn complete_operation(
        &mut self,
        unix_millis: u64,
    ) -> Result<&SemanticProgressSnapshot, String> {
        self.bump_sequence()?;
        self.progress.active_operation = None;
        self.progress.planned_silence = None;
        self.progress.last_operation_progress_unix_millis = unix_millis;
        self.heartbeat(unix_millis);
        self.progress.validate()?;
        Ok(&self.progress)
    }

    pub fn heartbeats(&self) -> &HeartbeatSnapshot {
        &self.heartbeats
    }

    pub fn snapshot(&self) -> &SemanticProgressSnapshot {
        &self.progress
    }

    fn bump_sequence(&mut self) -> Result<(), String> {
        self.progress.sequence = self
            .progress
            .sequence
            .checked_add(1)
            .ok_or_else(|| "semantic progress sequence exhausted".to_string())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeat_never_advances_semantic_progress() {
        let mut tracker = SemanticProgressTracker::new(1_000, "clients-16").unwrap();
        let before = tracker.snapshot().clone();

        tracker.heartbeat(2_000);

        assert_eq!(tracker.heartbeats().agent_unix_millis, Some(2_000));
        assert_eq!(tracker.snapshot(), &before);
    }

    #[test]
    fn only_nonzero_work_advances_workload_epoch() {
        let mut tracker = SemanticProgressTracker::new(1_000, "clients-16").unwrap();
        assert!(tracker.advance_workload(1_100, 0).unwrap().is_none());
        assert_eq!(tracker.snapshot().workload_epoch, 0);

        tracker.advance_workload(1_200, 7).unwrap().unwrap();
        assert_eq!(tracker.snapshot().workload_epoch, 1);
        assert_eq!(tracker.snapshot().workload_units, 7);
        assert_eq!(tracker.snapshot().last_workload_progress_unix_millis, 1_200);
    }
}
