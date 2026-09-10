import assert from 'node:assert/strict';
import { existsSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
const worktree = process.env.RADIXDB_DOCS_WORKTREE;
const revision = process.env.RADIXDB_DOCS_REVISION
  ?? '40b1b3d13e050afa2666a0414b7215d5ac1452c0';
assert(worktree && path.isAbsolute(worktree),
  'Set RADIXDB_DOCS_WORKTREE to the absolute frozen worktree path');

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: worktree,
    encoding: 'utf8',
    timeout: 300000,
    maxBuffer: 32 * 1024 * 1024,
    ...options,
  });
  assert(!result.error, String(result.error));
  assert.equal(result.signal, null, `${command} was terminated by ${result.signal}`);
  return result;
}

function source(relative) {
  return readFileSync(path.join(worktree, relative), 'utf8');
}

const status = run('git', ['status', '--short']);
assert.equal(status.status, 0, status.stderr);
assert.equal(status.stdout, '');
const head = run('git', ['rev-parse', 'HEAD']);
assert.equal(head.status, 0, head.stderr);
assert.equal(head.stdout.trim(), revision);

const documents = [
  'internals/overview.md',
  'internals/storage.md',
  'internals/protocol.md',
];
const codeBlocks = body => [...body.matchAll(/^```[^\n]*\n([\s\S]*?)^```/gm)]
  .map(match => match[1]);
let blockCount = 0;
for (const document of documents) {
  const en = readFileSync(path.join(root, 'src/content/docs/en', document), 'utf8');
  const ru = readFileSync(path.join(root, 'src/content/docs/ru', document), 'utf8');
  const enBlocks = codeBlocks(en);
  assert.deepEqual(enBlocks, codeBlocks(ru), `EN/RU code mismatch: ${document}`);
  blockCount += enBlocks.length;
}

const ownership = readFileSync(path.join(root,
  'src/content/docs/en/internals/overview.md'), 'utf8');
for (const owner of [
  'radixdb-core', 'radixdb-sql', 'radixdb-functions', 'radixdb-storage',
  'radixdb-executor', 'radixdb-api', 'radixdb-protocol', 'radixdb-client',
  'radixdb-orm',
]) assert(ownership.includes(`\`${owner}\``), `ownership map misses ${owner}`);

const database = source('crates/radixdb-api/src/database.rs');
for (const marker of [
  'struct DatabaseOwner',
  'pub(crate) struct DatabaseInner',
  'Connection-local database state',
  'static DATABASE_REGISTRY',
]) assert(database.includes(marker), `database composition misses ${marker}`);

const dispatch = source('crates/radixdb-executor/src/dispatch/statement.rs');
for (const marker of [
  'dispatch_authorize_statement',
  'acquire_statement_visibility_fence',
  'create_statement_savepoint',
  'route_statement',
]) assert(dispatch.includes(marker), `statement path misses ${marker}`);

const protocol = source('crates/radixdb-protocol/src/lib.rs');
for (const marker of [
  'pub const PROTOCOL_VERSION: u16 = 17;',
  'pub const DEFAULT_MAX_FRAME_BYTES: u32 = 64 * 1024 * 1024;',
  'pub const DEFAULT_MAX_DECODED_BYTES: usize = 256 * 1024 * 1024;',
  'pub enum ClientMessage',
  'pub enum ServerMessage',
  'ColumnBatchV1',
  'BuildIdentityV1',
  'CompactionBackpressure',
]) assert(protocol.includes(marker), `protocol owner misses ${marker}`);

const serverSession = source('src/server/session.rs');
for (const marker of [
  'SessionState::AwaitHandshake',
  'SessionState::AwaitAuthentication',
  'SessionState::Ready',
  'first client message must be Handshake',
  'close or fetch the active cursor before executing another query',
]) assert(serverSession.includes(marker), `server session misses ${marker}`);

const control = source('crates/radixdb-storage/src/v6/control.rs');
assert(control.includes('pub const CONTROL_RECORD_BYTES: usize = 4096;'));
const checkpoint = source('crates/radixdb-storage/src/mvcc/engine/checkpoint.rs');
for (const marker of [
  'fn checkpoint_cycle',
  'seal_checkpoint_hot_buffers',
  'publish_checkpoint_generation',
]) assert(checkpoint.includes(marker), `checkpoint owner misses ${marker}`);

for (const owner of [
  'crates/radixdb-storage/src/v6/control.rs',
  'crates/radixdb-storage/src/v6/manifest/model.rs',
  'crates/radixdb-storage/src/v6/catalog_wal.rs',
  'crates/radixdb-storage/src/v6/data/model.rs',
  'crates/radixdb-storage/src/v6/index/model.rs',
  'crates/radixdb-storage/src/v6/reachability.rs',
]) assert(existsSync(path.join(worktree, owner)), `format owner is missing: ${owner}`);

const gates = [
  ['-p', 'radixdb-protocol', '--test', 'boundary'],
  ['-p', 'radixdb-executor', '--test', 'boundary',
    'parse_cache_dispatch_and_transaction_routing_have_one_owner', '--', '--exact'],
  ['-p', 'radixdb-executor', '--test', 'boundary',
    'session_and_query_local_state_have_distinct_lifetimes', '--', '--exact'],
  ['-p', 'radixdb-storage', '--test', 'atomic_generation_publication',
    'complete_generation_moves_every_member_then_publishes_control_and_runtime', '--', '--exact'],
  ['-p', 'radixdb-storage', '--test', 'atomic_generation_publication',
    'concurrent_publishers_serialize_and_stale_loser_stops_before_control_write', '--', '--exact'],
  ['-p', 'radixdb-storage', '--test', 'atomic_generation_publication',
    'corrupt_optional_retained_index_does_not_block_publication', '--', '--exact'],
  ['-p', 'radixdb-storage', '--test', 'atomic_generation_publication',
    'publisher_rejects_another_database_root_before_mutation', '--', '--exact'],
];
let engineTests = 0;
for (const args of gates) {
  const result = run('cargo', ['test', '--locked', ...args]);
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  const passed = result.stdout.match(/(\d+) passed/)?.[1];
  assert(passed, `cannot count tests for cargo ${args.join(' ')}`);
  engineTests += Number(passed);
}

console.log(JSON.stringify({
  revision,
  documents: documents.length * 2,
  code_blocks: blockCount,
  protocol_version: 17,
  engine_tests: engineTests,
  passed: true,
}, null, 2));
