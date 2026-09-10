// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#![cfg(feature = "stress-tests")]

mod common;

#[test]
fn messenger_multi_client_commit_rollback_disconnect_checkpoint_chaos() {
    let _summary = common::prerelease::run_historical_messenger_chaos();
}

#[cfg(feature = "test-mutations")]
#[test]
fn historical_mixed_epoch_oracle_rejects_disabled_visibility_fence() {
    common::prerelease::prove_historical_mixed_epoch_oracle_rejects_disabled_fence();
}
