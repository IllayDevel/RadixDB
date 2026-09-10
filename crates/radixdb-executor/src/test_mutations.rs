// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Feature-gated mutation controls owned by executor tests.

use std::{
    sync::{Condvar, LazyLock, Mutex},
    time::{Duration, Instant},
};

const MUTATION_ENV: &str = "RADIXDB_TEST_MUTATION";

#[doc(hidden)]
pub fn constraint_validation_disabled() -> bool {
    std::env::var(MUTATION_ENV).is_ok_and(|name| name == "constraint_validation")
}

#[derive(Default)]
struct JoinPauseState {
    active: bool,
    reached: bool,
    released: bool,
    hits: u64,
}

static JOIN_PAUSE: LazyLock<(Mutex<JoinPauseState>, Condvar)> =
    LazyLock::new(|| (Mutex::new(JoinPauseState::default()), Condvar::new()));

pub struct JoinBetweenSourcesPause {
    armed: bool,
}

impl JoinBetweenSourcesPause {
    pub fn install() -> Result<Self, String> {
        let (state, _) = &*JOIN_PAUSE;
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.active {
            return Err("join-between-sources mutation is already armed".to_string());
        }
        *state = JoinPauseState {
            active: true,
            reached: false,
            released: false,
            hits: 0,
        };
        Ok(Self { armed: true })
    }

    pub fn wait_until_reached(&self, timeout: Duration) -> Result<(), String> {
        let (state, condition) = &*JOIN_PAUSE;
        let deadline = Instant::now() + timeout;
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while !state.reached {
            let now = Instant::now();
            if now >= deadline {
                return Err("join-between-sources mutation was not reached".to_string());
            }
            let remaining = deadline.saturating_duration_since(now);
            let (next, wait) = condition
                .wait_timeout(state, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state = next;
            if wait.timed_out() && !state.reached {
                return Err("join-between-sources mutation timed out".to_string());
            }
        }
        Ok(())
    }

    pub fn release(&self) {
        let (state, condition) = &*JOIN_PAUSE;
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.released = true;
        condition.notify_all();
    }

    pub fn hit_count(&self) -> u64 {
        JOIN_PAUSE
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .hits
    }
}

impl Drop for JoinBetweenSourcesPause {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let (state, condition) = &*JOIN_PAUSE;
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.released = true;
        state.active = false;
        condition.notify_all();
    }
}

#[doc(hidden)]
pub fn pause_between_join_sources() {
    let (state, condition) = &*JOIN_PAUSE;
    let mut state = state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if !state.active || state.reached {
        return;
    }
    state.reached = true;
    state.hits = state.hits.saturating_add(1);
    condition.notify_all();
    while state.active && !state.released {
        state = condition
            .wait(state)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
    }
}
