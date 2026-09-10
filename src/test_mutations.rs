// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Deliberately incorrect, feature-gated execution controls for mutation proof.
//!
//! This module is compiled only with `test-mutations`. It must never be enabled
//! in a release artifact. Each guard owns one bounded mutation and restores the
//! normal contract on drop.

/// Process-local selector used only by the prerelease mutation-proof runner.
/// The whole module is absent unless the `test-mutations` feature is enabled.
const MUTATION_ENV: &str = "RADIXDB_TEST_MUTATION";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mutation {
    SessionCleanup,
}

impl Mutation {
    const fn name(self) -> &'static str {
        match self {
            Self::SessionCleanup => "session_cleanup",
        }
    }
}

fn selected(mutation: Mutation) -> bool {
    std::env::var(MUTATION_ENV).is_ok_and(|name| name == mutation.name())
}

pub(crate) fn session_cleanup_disabled() -> bool {
    selected(Mutation::SessionCleanup)
}

pub use radixdb_executor::test_mutations::JoinBetweenSourcesPause;
pub use radixdb_storage::test_mutations::StatementVisibilityFenceMutation;
