import assert from 'node:assert/strict';
import { readFileSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

const cli = process.env.RADIXDB_DOCS_CLI;
const worktree = process.env.RADIXDB_DOCS_WORKTREE;
const revision = process.env.RADIXDB_DOCS_REVISION
  ?? '40b1b3d13e050afa2666a0414b7215d5ac1452c0';
assert(cli && path.isAbsolute(cli), 'Set RADIXDB_DOCS_CLI to the pinned binary');
assert(worktree && path.isAbsolute(worktree), 'Set RADIXDB_DOCS_WORKTREE to the pinned worktree');

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
  const source = readFileSync(new URL(`../src/content/docs/${locale}/sql/navigable-references.md`, import.meta.url), 'utf8');
  return [...source.matchAll(/^```sql\n([\s\S]*?)^```/gm)].map(match => match[1]);
}

function execute(database, sql, expectedDiagnostic) {
  const result = run(cli, ['-q', '-j', '--limit=1000', '-d', `file://${database}?sync_mode=full`, `--execute=${sql}`], {
    timeout: 30000,
  });
  if (expectedDiagnostic) {
    assert.notEqual(result.status, 0, `Expected rejection: ${sql}`);
    const diagnostic = `${result.stdout}\n${result.stderr}`.toLowerCase();
    assert(diagnostic.includes(expectedDiagnostic.toLowerCase()), diagnostic);
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

function cargoTest(target, testName) {
  const result = run('cargo', ['test', '--locked', '--test', target, testName, '--', '--exact'], {
    cwd: worktree,
  });
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
}

const sql = blocks('en');
assert.deepEqual(sql, blocks('ru'));
assert.equal(sql.length, 14);
const root = mkdtempSync(path.join(tmpdir(), 'radixdb-docs-navigation-'));
const database = path.join(root, 'database');

try {
  assert.deepEqual(
    execute(database, sql[0]).map(item => item.rows_affected),
    [0, 0, 0, 2, 3, 5],
  );

  const navigation = values(execute(database, sql[1]))[0];
  assert.deepEqual(navigation, [
    ['100', 'Alice', 'Finance', 'FIN'],
    ['101', 'Bob', 'Finance', 'FIN'],
    ['102', 'Carol', 'Engineering', 'ENG'],
    ['103', 'Dave', null, null],
    ['104', 'Eve', 'Unclassified', 'UNC'],
  ]);
  assert.deepEqual(values(execute(database, sql[2]))[0], navigation);
  assert.deepEqual(values(execute(database, sql[3]))[0], [
    ['100', 'Finance', 'FIN'],
    ['101', 'Finance', 'FIN'],
    ['102', 'Engineering', 'ENG'],
    ['103', null, null],
    ['104', 'Unclassified', 'UNC'],
  ]);
  assert.deepEqual(values(execute(database, sql[4]))[0], [
    ['100', 'Alice', 'Finance', 'Finance profile'],
    ['101', 'Bob', 'Finance', 'Finance profile'],
    ['102', 'Carol', 'Engineering', 'Engineering profile'],
    ['103', 'Dave', null, null],
    ['104', 'Eve', 'Unclassified', null],
  ]);
  assert.deepEqual(values(execute(database, sql[5]))[0], [
    ['100', 'Alice'], ['101', 'Bob'], ['103', 'Dave'], ['104', 'Eve'],
  ]);
  assert.deepEqual(values(execute(database, sql[6]))[0], [
    ['Engineering', '1', '110'],
    ['Finance', '2', '210'],
    ['Unclassified', '1', '80'],
  ]);

  const plan = values(execute(database, sql[7]))[0].flat().join('\n');
  for (const marker of [
    'Reference Navigation', 'Semantics: LEFT', 'paths_planned=1',
    'paths_executed=1', 'lookup_batches=', 'Integrity Check: enabled',
    'Actual Strategy:',
  ]) assert(plan.includes(marker), `EXPLAIN marker missing: ${marker}\n${plan}`);

  for (const [index, diagnostic] of [
    [8, 'NAVIGATION_AMBIGUOUS_ROOT'],
    [9, 'NAVIGATION_NOT_A_REFERENCE'],
    [10, 'NAVIGATION_TARGET_COLUMN_NOT_FOUND'],
    [11, 'NAVIGATION_READ_ONLY'],
    [13, 'NAVIGATION_UNSUPPORTED_REFERENCE_SHAPE'],
  ]) execute(database, sql[index], diagnostic);

  assert.deepEqual(values(execute(database, 'SELECT name FROM employees WHERE id = 100;'))[0], [['Alice']]);
  const explicitWrite = execute(database, sql[12]);
  assert.deepEqual(explicitWrite.filter(item => 'rows_affected' in item), [{ rows_affected: 1 }]);
  assert.deepEqual(values(explicitWrite), [[['Finance and Legal']]]);

  const views = values(execute(database, 'SHOW VIEWS;'))[0];
  assert(!views.some(row => row.includes('employee_departments')));

  const navigationSource = readFileSync(path.join(worktree,
    'crates/radixdb-executor/src/navigation/mod.rs'), 'utf8');
  assert(navigationSource.includes('const MAX_NAVIGATION_STEPS: usize = 8'));
  assert(navigationSource.includes('pub const MAX_NAVIGATION_PATHS: usize = 256'));
  assert(navigationSource.includes('const MAX_NAVIGATION_EDGES: usize = 512'));

  cargoTest('navigable_references_contract_test',
    'navigation_executes_through_unique_not_null_target');
  cargoTest('navigable_references_contract_test',
    'current_ddl_rejects_non_unique_and_composite_fk_shapes');
  cargoTest('navigable_references_schema_test',
    'reference_descriptor_rejects_nullable_unique_targets');
  cargoTest('navigable_references_transitive_test',
    'explicit_depth_eight_and_finite_self_cycle_match_left_join_chain');
  cargoTest('navigable_references_contexts_test',
    'grouping_having_aggregate_distinct_and_order_use_one_reference_edge');
  cargoTest('navigable_references_dml_test',
    'navigation_is_rejected_across_every_write_expression_context');
  cargoTest('navigable_references_dml_test',
    'reverse_collection_guess_is_rejected_without_touching_rows');
  cargoTest('navigable_references_storage_test',
    'reference_projection_matches_hot_cold_hybrid_and_wal_reopen');
  cargoTest('navigable_references_tcp_test',
    'embedded_tcp_and_prepared_navigation_have_identical_values_and_metadata');

  const cleanAfter = run('git', ['status', '--short'], { cwd: worktree, timeout: 15000 });
  assert.equal(cleanAfter.status, 0, cleanAfter.stderr);
  assert.equal(cleanAfter.stdout, '');

  console.log(JSON.stringify({
    identity: identity.stdout.trim(),
    published_blocks: sql.length,
    successful_blocks: 9,
    expected_errors: 5,
    engine_tests: 9,
    passed: true,
  }, null, 2));
} finally {
  rmSync(root, { recursive: true, force: true });
}
