import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, readdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
const worktree = process.env.RADIXDB_DOCS_WORKTREE;
const cli = process.env.RADIXDB_DOCS_CLI;
const revision = process.env.RADIXDB_DOCS_REVISION;
for (const [name, value] of [
  ['RADIXDB_DOCS_WORKTREE', worktree],
  ['RADIXDB_DOCS_CLI', cli],
]) assert(value && path.isAbsolute(value), `Set ${name} to an absolute pinned path`);
assert(revision && /^[0-9a-f]{40}$/.test(revision),
  'Set RADIXDB_DOCS_REVISION to the pinned 40-hex revision');

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    encoding: 'utf8', timeout: 300000, maxBuffer: 64 * 1024 * 1024, ...options,
  });
  assert(!result.error, String(result.error));
  assert.equal(result.signal, null, `${command} was terminated by ${result.signal}`);
  return result;
}

function cleanWorktree() {
  const result = run('git', ['status', '--short'], { cwd: worktree, timeout: 15000 });
  assert.equal(result.status, 0, result.stderr);
  assert.equal(result.stdout, '');
}

function page(locale, name) {
  return readFileSync(path.join(root, 'src/content/docs', locale,
    `administration/${name}.md`), 'utf8');
}

function codeBlocks(source) {
  return [...source.matchAll(/^```([^\n]*)\n([\s\S]*?)^```/gm)]
    .map(match => ({ language: match[1], body: match[2] }));
}

function occurrences(source, needle) {
  return source.split(needle).length - 1;
}

function rustSources(directory) {
  return readdirSync(directory, { withFileTypes: true }).flatMap(entry => {
    const target = path.join(directory, entry.name);
    if (entry.isDirectory()) return rustSources(target);
    return entry.name.endsWith('.rs') ? [readFileSync(target, 'utf8')] : [];
  });
}

function execute(database, sql) {
  const dsn = `file://${database}?sync_mode=full&page_cache_level=0` +
    '&volume_cache_bytes=1';
  const result = run(cli, ['-q', '-j', '--limit=100', '-d', dsn, `--execute=${sql}`], {
    timeout: 30000,
  });
  assert.equal(result.status, 0, result.stderr);
  return result.stdout.trim().split('\n').filter(Boolean).map(line => JSON.parse(line));
}

function resultValue(result) {
  assert.equal(result.count, 1);
  return result.rows[0][0].value;
}

function cargo(args) {
  const result = run('cargo', ['test', '--locked', ...args], { cwd: worktree });
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  return result;
}

function passedTests(result) {
  const counts = [...result.stdout.matchAll(/test result: ok\. (\d+) passed/g)]
    .map(match => Number(match[1]));
  assert(counts.length > 0, result.stdout);
  return Math.max(...counts);
}

cleanWorktree();
const identity = run(cli, ['--version'], { timeout: 15000 });
assert.equal(identity.status, 0, identity.stderr);
assert(identity.stdout.includes(`git=${revision} `), identity.stdout);
const head = run('git', ['rev-parse', 'HEAD'], { cwd: worktree, timeout: 15000 });
assert.equal(head.status, 0, head.stderr);
assert.equal(head.stdout.trim(), revision);

for (const name of ['storage', 'memory']) {
  assert.deepEqual(codeBlocks(page('en', name)), codeBlocks(page('ru', name)),
    `${name} code blocks differ between locales`);
}

const config = readFileSync(path.join(worktree, 'crates/radixdb-storage/src/config.rs'), 'utf8');
for (const contract of [
  'pub const DEFAULT_PAGE_CACHE_LEVEL: u8 = 0;',
  'pub const MAX_PAGE_CACHE_LEVEL: u8 = 10;',
  'pub volume_cache_bytes: usize,',
  'volume_cache_bytes: 1024 * 1024 * 1024,',
  'target_volume_rows: 1_048_576',
]) assert(config.includes(contract), `missing storage contract: ${contract}`);

const column = readFileSync(path.join(worktree,
  'crates/radixdb-storage/src/volume/column.rs'), 'utf8');
const dataModel = readFileSync(path.join(worktree,
  'crates/radixdb-storage/src/v6/data/model.rs'), 'utf8');
const physical = readFileSync(path.join(worktree,
  'crates/radixdb-storage/src/v6/data/physical.rs'), 'utf8');
assert(column.includes('pub const ROW_GROUP_SIZE: usize = 65536'));
assert(dataModel.includes('pub const MAX_ROWS_PER_GROUP: u32 = 65_536'));
assert(physical.includes('DataPhysicalCodec::Lz4 => lz4_flex::block::compress(logical)'));

const storageSource = rustSources(path.join(worktree, 'crates/radixdb-storage/src')).join('\n');
for (const retired of [
  'record_artifact_block_cache_hit',
  'record_artifact_block_cache_miss',
  'record_artifact_block_cache_insert',
  'record_artifact_scan_prefetch_schedule',
  'record_artifact_scan_prefetch_group',
]) assert.equal(occurrences(storageSource, retired), 0, `${retired} must be retired`);

const openSource = readFileSync(path.join(worktree,
  'crates/radixdb-storage/src/mvcc/engine/open.rs'), 'utf8');
assert(!openSource.includes('block_cache_resident_bytes'));
assert(!openSource.includes('scan_prefetch_in_flight_bytes'));

for (const runtimeFile of [
  'crates/radixdb-storage/src/mvcc/engine/seal.rs',
  'crates/radixdb-storage/src/mvcc/engine/compaction.rs',
  'crates/radixdb-storage/src/v6/data/physical.rs',
]) {
  const source = readFileSync(path.join(worktree, runtimeFile), 'utf8');
  assert(!source.includes('compression_threshold'), `${runtimeFile} now uses compression_threshold`);
}
const cliSource = readFileSync(path.join(worktree, 'src/cli/mod.rs'), 'utf8');
assert(!cliSource.includes('compression_threshold'));

const resourceReport = [
  'doc/public/evidence/performance/CA_80_5C_MICRO_DEVICE_20K_REPORT.md',
  'doc/public/evidence/performance/CA_80_7_100M_COMPARATIVE_REPORT.md',
  'doc/public/evidence/reliability/CA_90_3_6H_ACCEPTANCE_REPORT.md',
].map(relative => readFileSync(path.join(worktree, relative), 'utf8')).join('\n');
for (const evidence of [
  '1 060 020 224',
  '206 327 808',
  '24 571 904 B',
  '56 848 384 B',
  '1,964,220,021 B',
  '16,126,596,799 B',
]) assert(resourceReport.includes(evidence), `missing recorded evidence: ${evidence}`);

const temp = mkdtempSync(path.join(tmpdir(), 'radixdb-docs-storage-memory-'));
try {
  const database = path.join(temp, 'database');
  const setup = execute(database,
    'CREATE TABLE items (id INTEGER PRIMARY KEY); ' +
    'INSERT INTO items VALUES (1), (2); PRAGMA CHECKPOINT;');
  assert.deepEqual(setup.slice(0, 2).map(item => item.rows_affected), [0, 2]);
  assert.equal(resultValue(setup[2]), 'Checkpoint completed successfully');

  let executableSqlBlocks = 0;
  for (const name of ['storage', 'memory']) {
    for (const block of codeBlocks(page('en', name)).filter(item => item.language === 'sql')) {
      const results = execute(database, block.body);
      assert(results.length > 0, `${name} SQL block returned no result`);
      executableSqlBlocks += 1;
    }
  }

  const observed = execute(database,
    'SELECT * FROM items ORDER BY id; SELECT * FROM items ORDER BY id; ' +
    'PRAGMA VOLUME_STATS; PRAGMA RUNTIME_STATS;');
  assert.deepEqual(observed[0].rows.map(row => row[0].value), ['1', '2']);
  assert.deepEqual(observed[1].rows.map(row => row[0].value), ['1', '2']);
  assert(observed[2].columns.includes('column_payload_bytes'));
  const runtime = JSON.parse(resultValue(observed[3]));
  assert.equal(runtime.cold_rows, 2);
  for (const retired of [
    'block_cache_budget_bytes', 'block_cache_resident_bytes', 'block_cache_entries',
    'scan_prefetch_budget_bytes', 'scan_prefetch_in_flight_bytes',
  ]) assert(!(retired in runtime), `runtime must not advertise ${retired}`);
  for (const retired of [
    'artifact_block_cache_hits', 'artifact_block_cache_misses',
    'artifact_block_cache_insert_bytes', 'artifact_scan_prefetch_schedules',
    'artifact_scan_prefetch_decoded_groups',
  ]) assert(!(retired in runtime.counters), `counters must not advertise ${retired}`);

  const pageCacheTests = passedTests(cargo(['-p', 'radixdb-storage', 'page_cache::tests::']));
  const rowIdTests = passedTests(cargo(['-p', 'radixdb-storage', '--test', 'data_row_id_blocks']));
  const columnTests = passedTests(cargo(['-p', 'radixdb-storage', '--test', 'data_column_blocks']));

  cleanWorktree();
  console.log(JSON.stringify({
    identity: identity.stdout.trim(),
    paired_pages: 2,
    executable_sql_blocks: executableSqlBlocks,
    page_cache_tests: pageCacheTests,
    data_row_id_block_tests: rowIdTests,
    data_column_block_tests: columnTests,
    recorded_profiles_checked: 3,
    passed: true,
  }, null, 2));
} finally {
  rmSync(temp, { recursive: true, force: true });
}
