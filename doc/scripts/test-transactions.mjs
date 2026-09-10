import assert from 'node:assert/strict';
import { readFileSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

const cli = process.env.RADIXDB_DOCS_CLI;
const worktree = process.env.RADIXDB_DOCS_WORKTREE;
const revision = process.env.RADIXDB_DOCS_REVISION;
assert(cli && path.isAbsolute(cli), 'Set RADIXDB_DOCS_CLI to the pinned binary');
assert(worktree && path.isAbsolute(worktree), 'Set RADIXDB_DOCS_WORKTREE to the pinned worktree');
assert(revision && /^[0-9a-f]{40}$/.test(revision),
  'Set RADIXDB_DOCS_REVISION to the pinned 40-hex revision');

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    encoding: 'utf8', timeout: 120000, maxBuffer: 16 * 1024 * 1024, ...options,
  });
  assert(!result.error, String(result.error));
  assert.equal(result.signal, null, `${command} was terminated by ${result.signal}`);
  return result;
}

const identity = run(cli, ['--version'], { timeout: 15000 });
assert.equal(identity.status, 0, identity.stderr);
assert(identity.stdout.includes(`git=${revision} `), identity.stdout);

const head = run('git', ['rev-parse', 'HEAD'], { cwd: worktree, timeout: 15000 });
assert.equal(head.status, 0, head.stderr);
assert.equal(head.stdout.trim(), revision);
const cleanBefore = run('git', ['status', '--short'], { cwd: worktree, timeout: 15000 });
assert.equal(cleanBefore.status, 0, cleanBefore.stderr);
assert.equal(cleanBefore.stdout, '');

function blocks(locale) {
  const source = readFileSync(new URL(`../src/content/docs/${locale}/sql/transactions.md`, import.meta.url), 'utf8');
  return [...source.matchAll(/^```sql\n([\s\S]*?)^```/gm)].map(match => match[1]);
}

function execute(database, sql, expectedDiagnostic) {
  const result = run(cli, ['-q', '-j', '-d', `file://${database}?sync_mode=full`, `--execute=${sql}`], {
    timeout: 30000,
  });
  if (expectedDiagnostic) {
    assert.notEqual(result.status, 0, `Expected rejection: ${sql}`);
    assert(result.stderr.toLowerCase().includes(expectedDiagnostic), result.stderr);
    return [];
  }
  assert.equal(result.status, 0, result.stderr);
  return result.stdout.trim().split('\n').filter(Boolean).map(line => JSON.parse(line));
}

function values(output) {
  return output.filter(item => Array.isArray(item.rows)).map(result => {
    assert.equal(result.truncated, false);
    assert.equal(result.count, result.rows.length);
    return result.rows.map(row => row.map(cell => cell.value));
  });
}

function cargoTest(target, testName, features = []) {
  const args = ['test', '--locked', ...features, '--test', target, testName, '--', '--exact'];
  const result = run('cargo', args, { cwd: worktree });
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
}

const sql = blocks('en');
assert.deepEqual(sql, blocks('ru'));
assert.equal(sql.length, 8);
const root = mkdtempSync(path.join(tmpdir(), 'radixdb-docs-transactions-'));

try {
  const accountDatabase = path.join(root, 'accounts');
  const committed = execute(accountDatabase, sql[0]);
  assert.deepEqual(values(committed), [
    [['1', '75'], ['2', '85']],
    [['160']],
  ]);
  assert.deepEqual(
    committed.filter(item => 'rows_affected' in item).map(item => item.rows_affected),
    [0, 2, 1, 1],
  );

  const rolledBack = execute(accountDatabase, sql[1]);
  assert.deepEqual(values(rolledBack), [[['1', '75'], ['2', '85']]]);
  assert.deepEqual(
    rolledBack.filter(item => 'rows_affected' in item).map(item => item.rows_affected),
    [1],
  );

  const isolationDatabase = path.join(root, 'isolation');
  assert.deepEqual(
    execute(isolationDatabase, sql[3]),
    [{ rows_affected: 0 }, { rows_affected: 2 }],
  );

  const ignoredLevel = execute(path.join(root, 'cli-isolation'),
    'BEGIN ISOLATION LEVEL SERIALIZABLE; ROLLBACK;',
    'supported: read committed, snapshot');
  assert.deepEqual(ignoredLevel, []);
  const savepoint = execute(path.join(root, 'cli-savepoint'),
    'BEGIN; SAVEPOINT docs_probe; ROLLBACK TO SAVEPOINT docs_probe; ' +
    'RELEASE SAVEPOINT docs_probe; ROLLBACK;');
  assert.deepEqual(savepoint, []);

  const dispatch = readFileSync(path.join(worktree,
    'crates/radixdb-executor/src/dispatch/transaction.rs'), 'utf8');
  assert(dispatch.includes('"READ COMMITTED" => Ok(IsolationLevel::ReadCommitted)'));
  assert(dispatch.includes('"SNAPSHOT" => Ok(IsolationLevel::SnapshotIsolation)'));
  assert(dispatch.includes('Supported: READ COMMITTED, SNAPSHOT'));

  const parser = readFileSync(path.join(worktree,
    'crates/radixdb-sql/src/statements/control.rs'), 'utf8');
  for (const spelling of ['SERIALIZABLE', 'REPEATABLE READ', 'READ UNCOMMITTED', 'READ COMMITTED']) {
    assert(parser.includes(spelling), `parser evidence missing ${spelling}`);
  }

  const utility = readFileSync(path.join(worktree,
    'crates/radixdb-executor/src/query/utility.rs'), 'utf8');
  assert(utility.includes('self.set_default_isolation_level(isolation)'));
  assert(!utility.includes('self.engine.registry().set_global_isolation_level(isolation)'));

  const cliSource = readFileSync(path.join(worktree, 'src/cli/mod.rs'), 'utf8');
  assert(cliSource.includes('Statement::Begin(statement)'));
  assert(cliSource.includes('.begin_with_isolation(isolation)'));
  assert(cliSource.includes('.rollback_to_savepoint(name)'));
  assert(cliSource.includes('.release_savepoint(name)'));

  cargoTest('concurrency_history_test',
    'r3_l01_batch_c_history_matches_declared_isolation_boundaries',
    ['--features', 'stress-tests']);
  cargoTest('volume_concurrency_test', 'test_pending_update_invisible_to_others');
  cargoTest('r3_l01_batch_b_statement_lifecycle_test',
    'r3_l01_batch_b_savepoint_identity_and_target_lifetime_are_stable');
  cargoTest('tcp_concurrent_row_update_test', 'r8_l01_batch_b_tcp_waiters_use_state_barriers');
  cargoTest('tcp_transaction_error_atomicity_test',
    'transaction_errors_are_explicit_rollback_capable_and_cross_table_atomic');

  const cleanAfter = run('git', ['status', '--short'], { cwd: worktree, timeout: 15000 });
  assert.equal(cleanAfter.status, 0, cleanAfter.stderr);
  assert.equal(cleanAfter.stdout, '');

  console.log(JSON.stringify({
    identity: identity.stdout.trim(),
    published_blocks: sql.length,
    direct_cli_blocks: 3,
    engine_tests: 5,
    verified_cli_boundaries: 2,
    passed: true,
  }, null, 2));
} finally {
  rmSync(root, { recursive: true, force: true });
}
