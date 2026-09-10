import assert from 'node:assert/strict';
import {
  appendFileSync,
  chmodSync,
  cpSync,
  existsSync,
  lstatSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
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
  'Set RADIXDB_DOCS_REVISION to the pinned 40-hex revision');

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

function page(locale) {
  return readFileSync(path.join(root, 'src/content/docs', locale,
    'administration/backup-restore.md'), 'utf8');
}

function codeBlocks(source) {
  return [...source.matchAll(/^```([^\n]*)\n([\s\S]*?)^```/gm)]
    .map(match => ({ language: match[1], body: match[2] }));
}

function walk(entry) {
  const metadata = lstatSync(entry);
  if (!metadata.isDirectory()) return [{ path: entry, metadata }];
  return [{ path: entry, metadata }, ...readdirSync(entry)
    .flatMap(name => walk(path.join(entry, name)))];
}

function makeWritable(entry) {
  if (!existsSync(entry)) return;
  const metadata = lstatSync(entry);
  if (metadata.isDirectory()) {
    chmodSync(entry, 0o700);
    for (const name of readdirSync(entry)) makeWritable(path.join(entry, name));
  } else if (!metadata.isSymbolicLink()) {
    chmodSync(entry, 0o600);
  }
}

function execute(database, sql) {
  const dsn = `file://${database}?sync_mode=full&checkpoint_on_close=off`;
  const result = run(cli, [
    '--quiet', '--json', '--limit=100', '--db', dsn, '--execute', sql,
  ], { timeout: 60000 });
  assert.equal(result.status, 0, result.stderr);
  return result.stdout.trim().split('\n').filter(Boolean).map(line => JSON.parse(line));
}

function normalized(result) {
  return { columns: result.columns, rows: result.rows };
}

function cargo(args) {
  const result = run('cargo', ['test', '--locked', ...args], { cwd: worktree });
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
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

const enBlocks = codeBlocks(page('en'));
assert.deepEqual(enBlocks, codeBlocks(page('ru')));

const backupScript = path.join(worktree, 'release/backup-external.sh');
const restoreScript = path.join(worktree, 'release/restore-external.sh');
for (const script of [backupScript, restoreScript]) {
  const syntax = run('bash', ['-n', script]);
  assert.equal(syntax.status, 0, syntax.stderr);
}

const backupSource = readFileSync(backupScript, 'utf8');
const restoreSource = readFileSync(restoreScript, 'utf8');
assert(backupSource.includes('--snapshot'));
assert(backupSource.includes('cp -a -- "${SNAPSHOT_SOURCE}"'));
assert(backupSource.includes('format=radixdb-external-backup-v2'));
assert(backupSource.includes('git_commit='));
assert(backupSource.includes('cargo_lock_sha256='));
assert(backupSource.includes('physical_format='));
assert(backupSource.includes('database_id='));
assert(backupSource.includes('snapshot_id='));
assert(restoreSource.includes('cmp -s -- "${COMPUTED_SUMS}"'));
assert(restoreSource.includes('--restore "${SNAPSHOT_ID}"'));
assert(restoreSource.includes('exact backup snapshot is missing'));

const snapshotSource = readFileSync(path.join(worktree,
  'crates/radixdb-storage/src/mvcc/engine/physical_snapshot.rs'), 'utf8');
for (const contract of [
  'freeze_snapshot_generations',
  'commit_snapshot_manifest',
  'SnapshotIndexPolicy::Include',
  'manifest.created_unix_ns()',
  'keep_count.max(1)',
]) assert(snapshotSource.includes(contract), `missing snapshot contract: ${contract}`);

const dumpSource = readFileSync(path.join(worktree, 'src/sql_dump.rs'), 'utf8');
assert(dumpSource.includes('Physical database generations deliberately have no compatibility reader.'));

const help = run(cli, ['--help'], { timeout: 15000 });
assert.equal(help.status, 0, help.stderr);
assert(help.stdout.includes('--restore 0123456789abcdef0123456789abcdef'));
assert(help.stdout.includes('Number of complete database snapshots to keep'));

const manifestTests = cargo(['-p', 'radixdb-storage', '--test', 'physical_snapshot_manifest']);
const restoreTests = cargo(['-p', 'radixdb-storage', '--test', 'physical_snapshot_restore']);
const pragmaTests = cargo(['--test', 'pragma_restore_test']);

const temp = mkdtempSync(path.join(os.tmpdir(), 'radixdb-docs-backup-restore-'));
let summary;
try {
  const source = path.join(temp, 'source');
  const backup = path.join(temp, 'external-backup');
  const restored = path.join(temp, 'restored');

  const setup = execute(source,
    "CREATE TABLE items (id INTEGER PRIMARY KEY, code TEXT UNIQUE, rank INTEGER); " +
    'CREATE INDEX items_rank_idx ON items(rank); ' +
    "INSERT INTO items VALUES (1, 'alpha', 10), (2, 'beta', 20), (3, 'gamma', 30); " +
    'CREATE VIEW ranked_items AS SELECT id, code, rank FROM items WHERE rank >= 20;');
  assert.equal(setup.length, 4);

  const baselineRows = normalized(execute(source,
    'SELECT id, code, rank FROM items ORDER BY id;')[0]);
  const baselineIndexes = normalized(execute(source, 'SHOW INDEXES FROM items;')[0]);
  const baselineView = normalized(execute(source,
    'SELECT id, code, rank FROM ranked_items ORDER BY id;')[0]);
  assert.equal(baselineRows.rows.length, 3);
  assert(baselineIndexes.rows.length >= 3);
  assert.equal(baselineView.rows.length, 2);

  const created = run(backupScript, [source, backup], {
    env: { ...process.env, RADIXDB_CLI_BIN: cli },
  });
  assert.equal(created.status, 0, `${created.stdout}\n${created.stderr}`);
  assert(created.stdout.includes('immutable external backup '));
  assert(created.stdout.includes(' created: '));

  const backupTree = walk(backup);
  assert(backupTree.every(entry => !entry.metadata.isSymbolicLink()));
  assert(backupTree.every(entry => (entry.metadata.mode & 0o222) === 0));
  const snapshotIds = readdirSync(path.join(backup, 'snapshots'), { withFileTypes: true })
    .filter(entry => entry.isDirectory())
    .map(entry => entry.name);
  assert.equal(snapshotIds.length, 1);
  assert(snapshotIds.every(id => /^[0-9a-f]{32}$/.test(id)));
  const backupFiles = backupTree.filter(entry => entry.metadata.isFile());
  const walMembers = backupFiles.filter(entry =>
    entry.path.includes(`${path.sep}wal${path.sep}`)).length;
  assert(walMembers >= 1, 'physical snapshot did not retain a WAL suffix');
  assert.equal(backupFiles.filter(entry => path.basename(entry.path) === 'SNAPSHOT.mft').length,
    snapshotIds.length);
  const checksums = run('sha256sum', ['-c', 'SHA256SUMS'], { cwd: backup });
  assert.equal(checksums.status, 0, `${checksums.stdout}\n${checksums.stderr}`);
  const metadata = readFileSync(path.join(backup, 'BACKUP.env'), 'utf8');
  assert(metadata.includes('format=radixdb-external-backup-v2'));
  assert(metadata.includes(`git_commit=${revision}`));
  assert(metadata.includes(`snapshot_id=${snapshotIds[0]}`));

  execute(source,
    "UPDATE items SET rank = 999 WHERE id = 1; DELETE FROM items WHERE id = 2; " +
    "INSERT INTO items VALUES (4, 'delta', 40);");
  const changedRows = normalized(execute(source,
    'SELECT id, code, rank FROM items ORDER BY id;')[0]);
  assert.notDeepEqual(changedRows, baselineRows);

  const recovered = run(restoreScript, [backup, restored], {
    env: { ...process.env, RADIXDB_CLI_BIN: cli },
  });
  assert.equal(recovered.status, 0, `${recovered.stdout}\n${recovered.stderr}`);
  assert(recovered.stdout.includes('external backup '));
  assert(recovered.stdout.includes(' restored into new root: '));
  assert.deepEqual(normalized(execute(restored,
    'SELECT id, code, rank FROM items ORDER BY id;')[0]), baselineRows);
  assert.deepEqual(normalized(execute(restored, 'SHOW INDEXES FROM items;')[0]), baselineIndexes);
  assert.deepEqual(normalized(execute(restored,
    'SELECT id, code, rank FROM ranked_items ORDER BY id;')[0]), baselineView);

  const existingTarget = run(restoreScript, [backup, restored], {
    env: { ...process.env, RADIXDB_CLI_BIN: cli },
  });
  assert.notEqual(existingTarget.status, 0);
  assert(existingTarget.stderr.includes('restore target already exists'));

  const invalidSnapshotId = run(cli, [
    '--quiet', '--db', `file://${source}`, '--restore=20260315-100000',
  ]);
  assert.notEqual(invalidSnapshotId.status, 0);
  assert(invalidSnapshotId.stderr.includes('invalid physical snapshot ID'));

  const tampered = path.join(temp, 'tampered-backup');
  cpSync(backup, tampered, { recursive: true, preserveTimestamps: true });
  makeWritable(tampered);
  const tamperedMember = walk(path.join(tampered, 'snapshots'))
    .find(entry => entry.metadata.isFile() && path.basename(entry.path) !== 'SNAPSHOT.mft');
  assert(tamperedMember);
  appendFileSync(tamperedMember.path, 'tamper');
  const tamperedTarget = path.join(temp, 'tampered-target');
  const rejected = run(restoreScript, [tampered, tamperedTarget], {
    env: { ...process.env, RADIXDB_CLI_BIN: cli },
  });
  assert.notEqual(rejected.status, 0);
  assert(rejected.stderr.includes('inventory or checksum does not match'));
  assert(!existsSync(tamperedTarget));

  const checksumsAfterRestore = run('sha256sum', ['-c', 'SHA256SUMS'], { cwd: backup });
  assert.equal(checksumsAfterRestore.status, 0,
    `${checksumsAfterRestore.stdout}\n${checksumsAfterRestore.stderr}`);

  summary = {
    identity: identity.stdout.trim(),
    paired_code_blocks: enBlocks.length,
    physical_snapshot_manifest_tests: manifestTests,
    physical_snapshot_restore_tests: restoreTests,
    pragma_restore_tests: pragmaTests,
    external_backup_files: backupFiles.length,
    retained_snapshots: snapshotIds.length,
    retained_wal_members: walMembers,
    restored_rows: baselineRows.rows.length,
    restored_indexes: baselineIndexes.rows.length,
    restored_view_rows: baselineView.rows.length,
    existing_target_rejected: true,
    tampered_backup_rejected: true,
    source_and_backup_unchanged_by_restore: true,
    passed: true,
  };
} finally {
  makeWritable(temp);
  rmSync(temp, { recursive: true, force: true });
}

cleanWorktree();
console.log(JSON.stringify(summary, null, 2));
