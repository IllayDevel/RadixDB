#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_DIR}"

# The inventory and every historical reproduction live in this repository.
# The Messenger ticket directory is intentionally not read at runtime.
cargo test --locked --features stress-tests,orm-async-tests \
  --test server_identity_test \
  --test ddl_transaction_test \
  --test tcp_ddl_transaction_test \
  --test tcp_concurrent_row_update_test \
  --test tcp_transaction_error_atomicity_test \
  --test timestamp_index_plan_test \
  --test tcp_timestamp_index_plan_test \
  --test multi_index_or_plan_test \
  --test tcp_multi_index_or_plan_test \
  --test sequential_write_conflict_test \
  --test table_check_constraint_test \
  --test bound_uuid_in_test \
  --test cold_composite_index_test \
  --test cold_indexed_join_test \
  --test transaction_index_visibility_test \
  --test rdb_0022_check_lifecycle_test \
  --test rdb_0026_alter_foreign_key_test \
  --test rdb_0027_forwarded_attachment_join_test \
  --test rdb_0030_transactional_index_publication_test \
  --test rdb_0031_fk_parent_non_key_update_test \
  --test prerelease_concurrency_test \
  --test prerelease_ticket_belt_test \
  -- --test-threads=1

cargo test --locked -p radixdb-client --features tokio --lib -- --test-threads=1

cargo test --locked --lib \
  server::tcp_server::tests::r6_l01_a_disconnected_inflight_query_releases_connection_permit \
  -- --exact --test-threads=1

echo "prerelease Messenger ticket belt: PASS"
