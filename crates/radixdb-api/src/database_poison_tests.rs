// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;

#[test]
fn poisoned_connection_executor_fails_closed_across_public_facades() {
    let database = Database::open_in_memory().expect("open database");
    database
        .execute(
            "CREATE TABLE poisoned_executor (id INTEGER PRIMARY KEY)",
            (),
        )
        .expect("create table");
    let plan = database
        .cached_plan("SELECT id FROM poisoned_executor")
        .expect("cache plan");
    let context = crate::ServerExecutionContext::positional(crate::ParamVec::new());

    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _executor = database.inner.executor.lock().expect("lock executor");
        panic!("poison executor for fail-closed contract");
    }));
    assert!(poisoned.is_err());

    macro_rules! assert_executor_poisoned {
        ($operation:expr) => {
            match $operation {
                Err(Error::LockAcquisitionFailed(owner)) => assert_eq!(owner, "executor"),
                Err(error) => panic!("unexpected poisoned-executor error: {error}"),
                Ok(_) => panic!("poisoned executor operation unexpectedly succeeded"),
            }
        };
    }

    assert_executor_poisoned!(database.authenticate_principal("root", "password"));
    assert_executor_poisoned!(database.plugin_admission());
    assert_executor_poisoned!(database.plugin_admission_diagnostic());
    assert_executor_poisoned!(database.execute("SELECT 1", ()));
    assert_executor_poisoned!(database.query("SELECT 1", ()));
    assert_executor_poisoned!(database.execute_with_timeout("SELECT 1", (), 1_000));
    assert_executor_poisoned!(database.query_with_timeout("SELECT 1", (), 1_000));
    assert_executor_poisoned!(database.execute_named("SELECT 1", NamedParams::new()));
    assert_executor_poisoned!(database.query_named("SELECT 1", NamedParams::new()));
    assert_executor_poisoned!(database.query_named_with_timeout(
        "SELECT 1",
        NamedParams::new(),
        1_000,
    ));
    assert_executor_poisoned!(database.query_for_server("SELECT 1", &context));
    assert_executor_poisoned!(database.begin());
    assert_executor_poisoned!(database.begin_logical_export());
    assert_executor_poisoned!(database.begin_with_isolation(IsolationLevel::ReadCommitted));
    assert_executor_poisoned!(database.cached_plan("SELECT 1"));
    assert_executor_poisoned!(database.execute_plan(&plan, ()));
    assert_executor_poisoned!(database.query_plan(&plan, ()));
    assert_executor_poisoned!(database.execute_named_plan(&plan, NamedParams::new()));
    assert_executor_poisoned!(database.query_named_plan(&plan, NamedParams::new()));
    assert_executor_poisoned!(
        database.set_default_isolation_level(IsolationLevel::SnapshotIsolation)
    );
    assert_executor_poisoned!(database.default_isolation_level());
    assert_executor_poisoned!(database.describe_query_output("SELECT 1"));
    assert_executor_poisoned!(database.has_active_sql_transaction());
    assert_executor_poisoned!(database.semantic_cache_stats());
    assert_executor_poisoned!(database.clear_semantic_cache());
}
