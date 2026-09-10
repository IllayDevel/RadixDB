// Copyright 2026 RadixDB Contributors
// Licensed under the Apache License, Version 2.0.

#![cfg(all(feature = "stress-tests", feature = "test-failpoints"))]

mod common;

#[test]
fn epoch_change_chaos_preserves_atomic_durable_state() {
    common::prerelease::epoch_chaos::run().expect("epoch-change chaos gate");
}
