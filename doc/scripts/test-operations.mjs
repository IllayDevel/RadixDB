import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import {
  appendFileSync,
  existsSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import os from 'node:os';
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
  'Set RADIXDB_DOCS_REVISION to the pinned 40-hex worktree revision');

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    encoding: 'utf8',
    timeout: 300000,
    maxBuffer: 64 * 1024 * 1024,
    ...options,
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

function cargo(args) {
  const result = run('cargo', ['test', '--locked', ...args], { cwd: worktree });
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  const counts = [...`${result.stdout}\n${result.stderr}`.matchAll(
    /test result: ok\. (\d+) passed/g)].map(match => Number(match[1]));
  assert(counts.length > 0, result.stdout);
  return Math.max(...counts);
}

function parseCliJson(result) {
  assert.equal(result.status, 0, result.stderr);
  return result.stdout.trim().split('\n').filter(Boolean).map(line => JSON.parse(line));
}

function execute(database, sql) {
  return parseCliJson(run(cli, [
    '--quiet', '--json', '--limit=100',
    '--db', `file://${database}?sync_mode=full&checkpoint_on_close=off`,
    '--execute', sql,
  ], { timeout: 60000 }));
}

function treeDigest(entry) {
  const hash = createHash('sha256');
  function visit(current, relative = '') {
    for (const child of readdirSync(current, { withFileTypes: true })
      .sort((left, right) => left.name.localeCompare(right.name))) {
      const childRelative = path.join(relative, child.name);
      const childPath = path.join(current, child.name);
      if (child.isDirectory()) visit(childPath, childRelative);
      else if (child.isFile()) {
        if (childRelative === 'LOCK') continue;
        hash.update(childRelative);
        hash.update('\0');
        hash.update(readFileSync(childPath));
      }
    }
  }
  visit(entry);
  return hash.digest('hex');
}

cleanWorktree();
const identity = run(cli, ['--version'], { timeout: 15000 });
assert.equal(identity.status, 0, identity.stderr);
assert(identity.stdout.includes(`git=${revision} `), identity.stdout);
const head = run('git', ['rev-parse', 'HEAD'], { cwd: worktree, timeout: 15000 });
assert.equal(head.status, 0, head.stderr);
assert.equal(head.stdout.trim(), revision);

let pairedCodeBlocks = 0;
for (const name of ['upgrading', 'monitoring', 'troubleshooting']) {
  const en = page('en', name);
  const ru = page('ru', name);
  const enBlocks = codeBlocks(en);
  assert.deepEqual(enBlocks, codeBlocks(ru), `${name} code differs between locales`);
  pairedCodeBlocks += enBlocks.length;
}

const upgrading = page('en', 'upgrading');
for (const contract of [
  '--export-sql', '--import-sql', 'checkpoint_on_close=off',
  'test ! -e "$NEW_ROOT"', 'cmp --silent',
]) assert(upgrading.includes(contract), `upgrading omits ${contract}`);
const monitoring = page('en', 'monitoring');
for (const contract of [
  'server_status()', 'database_status(name)', 'PRAGMA RUNTIME_STATS',
  'complete = false', 'snapshots_omitted', 'disk_reserve_exhausted',
]) assert(monitoring.includes(contract), `monitoring omits ${contract}`);
const troubleshooting = page('en', 'troubleshooting');
for (const contract of [
  'ENOSPC', 'failed to sync WAL', 'Crash versus media loss',
  'sync_mode=normal', 'sync_mode=full', 'sync_mode=none',
]) assert(troubleshooting.includes(contract), `troubleshooting omits ${contract}`);

const dumpSource = readFileSync(path.join(worktree, 'src/sql_dump.rs'), 'utf8');
assert(dumpSource.includes('Physical database generations deliberately have no compatibility reader.'));
assert(dumpSource.includes('RENAME_NOREPLACE'));
const statusSource = readFileSync(path.join(worktree, 'src/server/session/status.rs'), 'utf8');
assert(statusSource.includes('collect_database_artifacts_recursive('));
assert(statusSource.includes('ARTIFACT_STATUS_MAX_ENTRIES'));
assert(statusSource.includes('ARTIFACT_STATUS_MAX_DEPTH'));
assert(statusSource.includes('child_in_snapshot'));
const protocolSource = readFileSync(path.join(worktree,
  'crates/radixdb-protocol/src/lib.rs'), 'utf8');
assert(protocolSource.includes('pub const PROTOCOL_VERSION: u16 = 17;'));
for (const typeName of [
  'ServerStatus', 'ServerRuntimeStatus', 'DatabaseStatus', 'DatabaseArtifactSummary',
]) assert(protocolSource.includes(`struct ${typeName}`), `missing ${typeName}`);
const runtimeSource = readFileSync(path.join(worktree,
  'crates/radixdb-storage/src/mvcc/engine/runtime.rs'), 'utf8');
assert(runtimeSource.includes('pub struct EngineRuntimeStatsV2'));
const engineSource = readFileSync(path.join(worktree,
  'crates/radixdb-storage/src/mvcc/engine/mod.rs'), 'utf8');
for (const limit of ['4_096', '16_384', '64']) assert(engineSource.includes(limit));
const configSource = readFileSync(path.join(worktree,
  'crates/radixdb-storage/src/config.rs'), 'utf8');
for (const contract of [
  'DEFAULT_COMPACTION_DISK_RESERVE_BYTES', 'DEFAULT_L0_SOFT_LIMIT_SEGMENTS',
  'DEFAULT_L0_HARD_LIMIT_SEGMENTS', 'SyncMode',
]) assert(configSource.includes(contract), `missing configuration contract ${contract}`);

const migrationOracleTests = cargo([
  '--features', 'cli', '--test', 'catalog_artifact_sql_migration_oracle',
]);
const migrationRehearsal = cargo([
  '--features', 'cli',
  '--test', 'catalog_artifact_migration_rehearsal',
  'real_messenger_shape_survives_source_free_process_migration', '--', '--exact',
]);
const serverStatusTests = cargo(['--test', 'server_status_test']);
const enospcTests = cargo([
  '--features', 'test-failpoints', '--test', 'failpoint_io_test',
  'test_test_only_enospc_is_explicit_atomic_and_reopenable',
]);
const recoveryTests = cargo(['-p', 'radixdb-storage', '--test', 'complete_recovery']);

const temp = mkdtempSync(path.join(os.tmpdir(), 'radixdb-docs-operations-'));
let summary;
try {
  const source = path.join(temp, 'source');
  const target = path.join(temp, 'target');
  const dump = path.join(temp, 'migration.sql');
  const reexport = path.join(temp, 'reexport.sql');
  const corruptDump = path.join(temp, 'corrupt.sql');

  const setup = execute(source,
    'CREATE TABLE accounts (id INTEGER PRIMARY KEY, code TEXT UNIQUE, balance INTEGER); ' +
    'CREATE INDEX accounts_balance_idx ON accounts(balance); ' +
    "INSERT INTO accounts VALUES (1, 'alpha', 100), (2, 'beta', 200); " +
    'CREATE VIEW positive_accounts AS SELECT id, code, balance FROM accounts WHERE balance > 0; ' +
    'PRAGMA CHECKPOINT;');
  assert.equal(setup.length, 5);
  const sourceRows = execute(source,
    'SELECT id, code, balance FROM accounts ORDER BY id;')[0];
  const sourceIndexes = execute(source, 'SHOW INDEXES FROM accounts;')[0];
  const sourceView = execute(source,
    'SELECT id, code, balance FROM positive_accounts ORDER BY id;')[0];
  const sourceBefore = treeDigest(source);

  const exported = run(cli, [
    '--quiet', '--db', `file://${source}?checkpoint_on_close=off`,
    '--export-sql', dump,
  ]);
  assert.equal(exported.status, 0, exported.stderr);
  assert.equal(treeDigest(source), sourceBefore, 'logical export mutated source files');
  const dumpText = readFileSync(dump, 'utf8');
  assert(dumpText.startsWith('-- radixdb-sql-dump: 1\n'));
  assert(dumpText.includes('-- radixdb-sql-dump-counts: '));
  assert(dumpText.includes('-- radixdb-sql-dump-sha256: '));

  assert(!existsSync(target));
  const imported = run(cli, [
    '--quiet', '--db', `file://${target}`, '--import-sql', dump,
  ]);
  assert.equal(imported.status, 0, imported.stderr);
  assert(existsSync(target));
  assert.deepEqual(execute(target,
    'SELECT id, code, balance FROM accounts ORDER BY id;')[0], sourceRows);
  assert.deepEqual(execute(target, 'SHOW INDEXES FROM accounts;')[0], sourceIndexes);
  assert.deepEqual(execute(target,
    'SELECT id, code, balance FROM positive_accounts ORDER BY id;')[0], sourceView);

  const reexported = run(cli, [
    '--quiet', '--db', `file://${target}?checkpoint_on_close=off`,
    '--export-sql', reexport,
  ]);
  assert.equal(reexported.status, 0, reexported.stderr);
  assert.deepEqual(readFileSync(reexport), readFileSync(dump));

  const occupied = path.join(temp, 'occupied');
  const created = execute(occupied,
    'CREATE TABLE sentinel (id INTEGER PRIMARY KEY); INSERT INTO sentinel VALUES (7);');
  assert.equal(created.length, 2);
  const occupiedBefore = treeDigest(occupied);
  const rejectedExisting = run(cli, [
    '--quiet', '--db', `file://${occupied}`, '--import-sql', dump,
  ]);
  assert.notEqual(rejectedExisting.status, 0);
  assert(rejectedExisting.stderr.includes('already exists'));
  assert.equal(treeDigest(occupied), occupiedBefore);

  writeFileSync(corruptDump, dumpText);
  appendFileSync(corruptDump, '-- tampered after checksum\n');
  const corruptTarget = path.join(temp, 'corrupt-target');
  const rejectedCorrupt = run(cli, [
    '--quiet', '--db', `file://${corruptTarget}`, '--import-sql', corruptDump,
  ]);
  assert.notEqual(rejectedCorrupt.status, 0);
  assert(!existsSync(corruptTarget));

  const runtimeDatabase = path.join(temp, 'runtime');
  const runtimeResults = execute(runtimeDatabase,
    'CREATE TABLE probe (id INTEGER PRIMARY KEY); INSERT INTO probe VALUES (1); ' +
    'PRAGMA RUNTIME_STATS; PRAGMA RUNTIME_STATS;');
  assert.equal(runtimeResults.length, 4);
  const stats = runtimeResults.slice(2).map(result => JSON.parse(result.rows[0][0].value));
  assert(stats.every(sample => sample.format === 2));
  assert(stats.every(sample => sample.complete === true));
  assert(stats[1].sequence > stats[0].sequence);
  assert.deepEqual(stats[0].owner_visit_limits, {
    max_tables: 4096,
    max_segments: 16384,
    max_transactions: 4096,
    max_staging_tables: 16384,
    max_active_segment_ids: 64,
  });
  assert.equal(stats[0].wal_running, true);

  summary = {
    identity: identity.stdout.trim(),
    paired_code_blocks: pairedCodeBlocks,
    migration_oracle_tests: migrationOracleTests,
    migration_rehearsal_tests: migrationRehearsal,
    server_status_tests: serverStatusTests,
    enospc_tests: enospcTests,
    complete_recovery_tests: recoveryTests,
    migrated_rows: sourceRows.rows.length,
    migrated_indexes: sourceIndexes.rows.length,
    migrated_view_rows: sourceView.rows.length,
    byte_identical_reexport: true,
    source_durable_artifacts_unchanged_by_export: true,
    existing_target_unchanged: true,
    corrupt_dump_target_absent: true,
    runtime_stats_format: stats[0].format,
    runtime_stats_complete: stats[0].complete,
    runtime_stats_visit_limits: stats[0].owner_visit_limits,
    passed: true,
  };
} finally {
  rmSync(temp, { recursive: true, force: true });
}

cleanWorktree();
console.log(JSON.stringify(summary, null, 2));
