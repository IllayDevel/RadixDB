import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
const worktree = process.env.RADIXDB_DOCS_WORKTREE;
const revision = process.env.RADIXDB_DOCS_REVISION;
assert(worktree && path.isAbsolute(worktree), 'Set RADIXDB_DOCS_WORKTREE to the pinned worktree');
assert(revision && /^[0-9a-f]{40}$/.test(revision),
  'Set RADIXDB_DOCS_REVISION to the pinned 40-hex revision');

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    encoding: 'utf8', timeout: 180000, maxBuffer: 32 * 1024 * 1024, ...options,
  });
  assert(!result.error, String(result.error));
  assert.equal(result.signal, null, `${command} was terminated by ${result.signal}`);
  return result;
}

const head = run('git', ['rev-parse', 'HEAD'], { cwd: worktree, timeout: 15000 });
assert.equal(head.status, 0, head.stderr);
assert.equal(head.stdout.trim(), revision);
const cleanBefore = run('git', ['status', '--short'], { cwd: worktree, timeout: 15000 });
assert.equal(cleanBefore.status, 0, cleanBefore.stderr);
assert.equal(cleanBefore.stdout, '');

function read(relative) {
  return readFileSync(path.join(worktree, relative), 'utf8');
}

function document(locale) {
  return readFileSync(path.join(root, 'src/content/docs', locale, 'appendices/limits.md'), 'utf8');
}

function rows(locale, prefix) {
  return document(locale).split('\n')
    .filter(line => line.startsWith(`| ${prefix}-`))
    .map(line => line.split('|').slice(1, -1).map(value => value.trim()));
}

function canonicalSource(value) {
  return value.replaceAll(/\[[^\]]+\]\(([^)]+)\)/g, (_match, target) =>
    `[](${target.replace(/\.ru\.md$/, '.md')})`);
}

for (const prefix of ['HARD', 'PLUG', 'CFG', 'SCALE']) {
  const en = rows('en', prefix);
  const ru = rows('ru', prefix);
  assert.deepEqual(en.map(row => row[0]), ru.map(row => row[0]), `${prefix} IDs differ`);
  assert.deepEqual(en.map(row => canonicalSource(row.at(-1))),
    ru.map(row => canonicalSource(row.at(-1))), `${prefix} sources differ`);
}
assert.equal(rows('en', 'HARD').length, 45);
assert.equal(rows('en', 'PLUG').length, 9);
assert.equal(rows('en', 'CFG').length, 13);
assert.equal(rows('en', 'SCALE').length, 3);

const sourceContracts = [
  ['crates/radixdb-catalog/src/codec/primitives.rs', [
    'MAX_CATALOG_FILE_BYTES: u64 = 512 * 1024 * 1024',
    'MAX_CATALOG_OBJECTS: u64 = 262_144',
    'MAX_CATALOG_EDGES: u64 = 1_048_576',
    'MAX_FIELDS_PER_OBJECT: u64 = 64',
    'MAX_PAYLOAD_BYTES_PER_OBJECT: u64 = 16 * 1024 * 1024',
  ]],
  ['crates/radixdb-catalog/src/name.rs', [
    'MAX_NORMALIZED_NAME_BYTES: usize = 1024',
    'MAX_DISPLAY_NAME_BYTES: usize = 4096',
  ]],
  ['crates/radixdb-catalog/src/payload/common.rs', [
    'MAX_CANONICAL_SQL_BYTES: usize = 16 * 1024 * 1024',
  ]],
  ['crates/radixdb-catalog/src/graph/validate.rs', [
    'MAX_DEPENDENCY_DEPTH: usize = 256',
  ]],
  ['crates/radixdb-catalog/src/payload/routine.rs', [
    'MAX_ROUTINE_ARGUMENTS: usize = 1024',
    'MAX_RESULT_COLUMNS: usize = 4096',
  ]],
  ['crates/radixdb-catalog/src/payload/job.rs', [
    'MAX_JOB_ARGUMENTS: usize = 1024',
    'MAX_LITERAL_BYTES: usize = 8 * 1024 * 1024',
  ]],
  ['crates/radixdb-catalog/src/payload/data_type.rs', [
    'MAX_VECTOR_DIMENSIONS: u32 = u16::MAX as u32',
  ]],
  ['crates/radixdb-executor/src/navigation/mod.rs', [
    'MAX_NAVIGATION_STEPS: usize = 8',
    'MAX_NAVIGATION_PATHS: usize = 256',
    'MAX_NAVIGATION_EDGES: usize = 512',
  ]],
  ['crates/radixdb-storage/src/v6/control.rs', ['CONTROL_RECORD_BYTES: usize = 4096']],
  ['crates/radixdb-storage/src/v6/manifest/model.rs', [
    'MAX_TABLES_PER_DATABASE: usize = 262_144',
    'MAX_SEGMENTS_PER_TABLE_MANIFEST: usize = 1_048_576',
    'MAX_ROWS_PER_DATA_ARTIFACT: u64 = u32::MAX as u64',
  ]],
  ['crates/radixdb-storage/src/v6/reachability.rs', [
    'MAX_REACHABLE_IDENTITIES: u64 = 8_388_608',
    'MAX_REACHABILITY_BYTES: u64 = 1024 * 1024 * 1024',
    'MAX_OPEN_METADATA_BYTES: u64 = 512 * 1024 * 1024',
    'MAX_TOTAL_MANIFESTS_PER_OPEN: u64 = 262_145',
    'MAX_TOTAL_SEGMENTS_PER_OPEN: u64 = 4_194_304',
  ]],
  ['crates/radixdb-storage/src/v6/artifact.rs', [
    'MAX_ARTIFACT_FILE_BYTES: u64 = 64 * 1024 * 1024 * 1024',
  ]],
  ['crates/radixdb-storage/src/v6/data/model.rs', [
    'MAX_COLUMNS_PER_TABLE: u32 = 4_096',
    'MAX_ROW_GROUPS_PER_DATA_ARTIFACT: u32 = 65_536',
    'MAX_ROWS_PER_GROUP: u32 = 65_536',
    'MAX_BLOCKS_PER_DATA_ARTIFACT: u64 = 4_194_304',
    'MAX_STORED_BYTES_PER_BLOCK: u64 = 256 * 1024 * 1024',
    'MAX_LOGICAL_BYTES_PER_BLOCK: u64 = 512 * 1024 * 1024',
  ]],
  ['crates/radixdb-storage/src/v6/data/column.rs', [
    'MAX_BYTES_PER_VALUE: usize = 256 * 1024 * 1024',
  ]],
  ['crates/radixdb-storage/src/v6/index/model.rs', [
    'MAX_ACCELERATORS_PER_INDEX_ARTIFACT: u32 = 4_096',
    'MAX_INDEX_SECTIONS: u32 = 16_384',
    'MAX_INDEX_PAGES: u64 = 4_194_304',
    'MAX_KEY_COLUMNS: u32 = 64',
    'MAX_ENTRIES_PER_INDEX_PAGE: u64 = 1_048_576',
    'MAX_STORED_BYTES_PER_INDEX_PAGE: u64 = 64 * 1024 * 1024',
    'MAX_LOGICAL_BYTES_PER_INDEX_PAGE: u64 = 256 * 1024 * 1024',
  ]],
  ['crates/radixdb-storage/src/v6/catalog_wal.rs', [
    'MAX_CATALOG_WAL_REPLAY_BYTES: u64 = 1024 * 1024 * 1024',
    'MAX_CATALOG_WAL_REPLAY_TRANSACTIONS: u64 = 262_144',
  ]],
  ['crates/radixdb-storage/src/v6/snapshot/model.rs', [
    'MAX_SNAPSHOT_MEMBERS: usize = 8_388_608',
    'MAX_SNAPSHOT_MANIFEST_BYTES: usize =',
  ]],
  ['crates/radixdb-protocol/src/lib.rs', [
    'DEFAULT_MAX_FRAME_BYTES: u32 = 64 * 1024 * 1024',
    'MIN_CONTROL_FRAME_BYTES: u32 = 256',
  ]],
  ['crates/radixdb-plugin-host/src/manifest.rs', [
    'MAX_MANIFEST_BYTES: u64 = 64 * 1024',
    'MAX_LIBRARY_BYTES: u64 = 256 * 1024 * 1024',
  ]],
  ['crates/radixdb-plugin-abi/src/constants.rs', [
    'RADIX_MAX_LOCAL_ID_BYTES: u32 = 255',
    'RADIX_MAX_DIAGNOSTIC_BYTES: u32 = 4 * 1024',
    'RADIX_MAX_EXTERNAL_VALUE_BYTES: u32 = 16 * 1024 * 1024',
    'RADIX_MAX_DESCRIPTOR_ENTRIES: u32 = u16::MAX as u32',
    'RADIX_MAX_FUNCTION_ARGUMENTS: u32 = 1024',
    'RADIX_MAX_PLANNER_SPANS: u32 = 4096',
    'RADIX_MAX_HASH_COMPONENTS: u32 = 256',
    'RADIX_MAX_HASH_BYTES: u32 = 64 * 1024',
  ]],
  ['crates/radixdb-storage/src/config.rs', [
    'DEFAULT_COPY_MAX_TRANSACTION_BYTES: usize = 512 * 1024 * 1024',
    'MAX_COMPACTION_JOBS: usize = 8',
    'MAX_PAGE_CACHE_LEVEL: u8 = 10',
  ]],
  ['src/server/config.rs', [
    'default_max_connections() -> usize',
    'default_max_connections() -> usize {\n    151',
    'target_volume_rows < 65_536',
    'cursor_batch_max_bytes > self.max_frame_bytes as usize',
  ]],
  ['release/server.toml', [
    'max_connections = 64',
    'max_inflight_frame_bytes = 268435456',
    'max_databases = 64',
    'max_database_name_bytes = 64',
    'cursor_batch_max_rows = 1024',
    'cursor_batch_max_bytes = 8388608',
    'max_frame_bytes = 67108864',
    'copy_max_transaction_bytes = 536870912',
    'target_volume_rows = 1048576',
  ]],
];

let sourceMarkers = 0;
for (const [relative, markers] of sourceContracts) {
  const source = read(relative);
  for (const marker of markers) {
    assert(source.includes(marker), `${relative} is missing ${marker}`);
    sourceMarkers += 1;
  }
}

const reportContracts = [
  ['doc/public/evidence/performance/CA_80_5C_MICRO_DEVICE_20K_REPORT.md', [
    'benchmark source: `1c604d3455056444508ad6fc7de82b3c0a575dd9`',
    'exactly `20 000` rows',
    '| peak/final RSS | 24 571 904 B | 56 848 384 B |',
  ]],
  ['doc/public/evidence/performance/CA_80_7_100M_COMPARATIVE_REPORT.md', [
    'candidate source: `b648b2d3ea323cf5eb10417ab09d5eb3d5d01ecc`',
    '| logical database | `1,964,220,021 B`',
    '| median | deliberately mixed, summary only | `8.947 s` | `547,303,424 B` | `293,380,096 B` |',
  ]],
  ['doc/public/evidence/reliability/CA_90_3_6H_ACCEPTANCE_REPORT.md', [
    '| Engine/soak Git SHA | `dd0bf75c9176bceb70ce8f1d2a07057610ec381b` |',
    '| Workload duration | `21 600 000 ms` |',
    '| Peak server RSS from `samples.jsonl` | `1 060 020 224` bytes',
  ]],
];

let reportMarkers = 0;
for (const [relative, markers] of reportContracts) {
  const report = read(relative);
  for (const marker of markers) {
    assert(report.includes(marker), `${relative} is missing ${marker}`);
    reportMarkers += 1;
  }
}

const storageTests = run('cargo', ['test', '--locked', '-p', 'radixdb-storage',
  '--test', 'limit_contract'], { cwd: worktree });
assert.equal(storageTests.status, 0, `${storageTests.stdout}\n${storageTests.stderr}`);

const serverTests = run('cargo', ['test', '--locked', '--lib',
  'server::config::tests'], { cwd: worktree });
assert.equal(serverTests.status, 0, `${serverTests.stdout}\n${serverTests.stderr}`);

const protocolTest = run('cargo', ['test', '--locked', '-p', 'radixdb-protocol', '--lib',
  'tests::shared_frame_helpers_preserve_limits_and_payload_contract', '--', '--exact'],
{ cwd: worktree });
assert.equal(protocolTest.status, 0, `${protocolTest.stdout}\n${protocolTest.stderr}`);

const cleanAfter = run('git', ['status', '--short'], { cwd: worktree, timeout: 15000 });
assert.equal(cleanAfter.status, 0, cleanAfter.stderr);
assert.equal(cleanAfter.stdout, '');

console.log(JSON.stringify({
  revision,
  documents: 2,
  hard_limits: rows('en', 'HARD').length,
  plugin_limits: rows('en', 'PLUG').length,
  configurable_limits: rows('en', 'CFG').length,
  measured_profiles: rows('en', 'SCALE').length,
  source_markers: sourceMarkers,
  report_markers: reportMarkers,
  engine_tests: 17,
  passed: true,
}, null, 2));
