import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

const revision = process.env.RADIXDB_DOCS_REVISION;
const cli = process.env.RADIXDB_DOCS_CLI;
const worktree = process.env.RADIXDB_DOCS_WORKTREE;
assert(cli && path.isAbsolute(cli), 'Set RADIXDB_DOCS_CLI to the pinned binary');
assert(worktree && path.isAbsolute(worktree), 'Set RADIXDB_DOCS_WORKTREE to the pinned worktree');
assert(revision && /^[0-9a-f]{40}$/.test(revision),
  'Set RADIXDB_DOCS_REVISION to the pinned 40-hex revision');

const range = (prefix, first, last) =>
  Array.from({ length: last - first + 1 }, (_, offset) =>
    `${prefix}-${String(first + offset).padStart(2, '0')}`);

const pages = new Map([
  ['select', [...range('QUERY', 1, 15), ...range('NAV', 1, 5), ...range('NAV', 7, 9)]],
  ['insert', ['DML-01', 'DML-02', ...range('DML', 5, 8), 'NAV-06']],
  ['update', ['DML-02', 'DML-03', 'DML-04', 'DML-07', 'DML-08', 'NAV-06']],
  ['delete', ['DML-02', 'DML-03', 'DML-04', 'DML-07', 'DML-08', 'NAV-06']],
  ['create-table', [...range('DDL', 1, 4), 'DDL-08']],
  ['alter-table', [...range('DDL', 6, 8)]],
  ['drop-table', ['DDL-08', 'DDL-09']],
  ['create-index', [...range('IDX', 1, 7)]],
  ['alter-index', ['IDX-08']],
  ['drop-index', ['IDX-09']],
  ['describe', ['DDL-05']],
  ['show', ['DDL-05', 'IDX-08']],
  ['begin', ['TX-01', ...range('TX', 3, 6)]],
  ['commit', ['TX-01']],
  ['rollback', ['TX-01', 'TX-02', 'TX-08', 'TX-09']],
  ['savepoint', ['TX-02', 'TX-06']],
  ['release-savepoint', ['TX-02', 'TX-06']],
  ['set', ['TX-07']],
]);

const headings = {
  en: ['Synopsis', 'Description', 'Parameters', 'Result', 'Transaction behavior',
    'Errors and limitations', 'Privileges', 'Example', 'See also'],
  ru: ['Синтаксис', 'Описание', 'Параметры', 'Результат', 'Поведение в транзакции',
    'Ошибки и ограничения', 'Права', 'Пример', 'См. также'],
};

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    encoding: 'utf8', timeout: 120000, maxBuffer: 16 * 1024 * 1024, ...options,
  });
  assert(!result.error, String(result.error));
  assert.equal(result.signal, null, `${command} was terminated by ${result.signal}`);
  return result;
}

function source(locale, slug) {
  return readFileSync(new URL(`../src/content/docs/${locale}/reference/sql/${slug}.md`, import.meta.url), 'utf8');
}

function sqlBlocks(text) {
  return [...text.matchAll(/^```sql\n([\s\S]*?)^```/gm)].map(match => match[1]);
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

const expectedMatrixIds = new Set([
  ...range('DDL', 1, 9), ...range('IDX', 1, 9), ...range('DML', 1, 8),
  ...range('QUERY', 1, 15), ...range('TX', 1, 9), ...range('NAV', 1, 9),
]);
const matrix = readFileSync(new URL('../src/content/docs/en/appendices/compatibility.md', import.meta.url), 'utf8');
const actualMatrixIds = new Set(
  [...matrix.matchAll(/^\| ((?:DDL|DML|IDX|QUERY|TX|NAV)-\d+) \|/gm)].map(match => match[1]),
);
assert.deepEqual([...actualMatrixIds].sort(), [...expectedMatrixIds].sort());

const documentedIds = new Set();
const examples = new Map();
for (const [slug, expectedCoverage] of pages) {
  const en = source('en', slug);
  const ru = source('ru', slug);
  for (const heading of headings.en) assert(en.includes(`## ${heading}`), `${slug}: missing EN ${heading}`);
  for (const heading of headings.ru) assert(ru.includes(`## ${heading}`), `${slug}: missing RU ${heading}`);
  const enBlocks = sqlBlocks(en);
  assert.equal(enBlocks.length, 1, `${slug}: expected one executable example`);
  assert.deepEqual(enBlocks, sqlBlocks(ru), `${slug}: SQL example parity`);
  examples.set(slug, enBlocks[0]);
  expectedCoverage.forEach(id => documentedIds.add(id));
}
assert.deepEqual([...documentedIds].sort(), [...expectedMatrixIds].sort());

const parserSources = [
  'crates/radixdb-sql/src/statements/control.rs',
  'crates/radixdb-sql/src/statements/ddl.rs',
  'crates/radixdb-sql/src/statements/dml.rs',
  'crates/radixdb-sql/src/statements/query.rs',
].map(file => readFileSync(path.join(worktree, file), 'utf8')).join('\n');
for (const marker of [
  'parse_select_statement', 'parse_insert_statement', 'parse_update_statement',
  'parse_delete_statement', 'parse_create_table_statement', 'parse_alter_table_statement',
  'parse_drop_table_statement', 'parse_create_index_statement',
  'parse_alter_index_statement', 'parse_drop_index_statement', 'parse_describe_statement',
  'parse_show_statement', 'parse_begin_statement', 'parse_commit_statement',
  'parse_rollback_statement', 'parse_savepoint_statement',
  'parse_release_savepoint_statement', 'parse_set_statement',
]) assert(parserSources.includes(marker), `parser evidence missing ${marker}`);

const dispatch = readFileSync(path.join(worktree,
  'crates/radixdb-executor/src/dispatch/statement.rs'), 'utf8');
for (const variant of [
  'Select', 'Insert', 'Update', 'Delete', 'CreateTable', 'AlterTable', 'DropTable',
  'CreateIndex', 'AlterIndex', 'DropIndex', 'Describe', 'Begin', 'Commit',
  'Rollback', 'Savepoint', 'ReleaseSavepoint', 'Set',
]) assert(dispatch.includes(`Statement::${variant}`), `dispatch evidence missing ${variant}`);
for (const variant of ['ShowTables', 'ShowViews', 'ShowCreateTable', 'ShowCreateView', 'ShowIndexes']) {
  assert(dispatch.includes(`Statement::${variant}`), `dispatch evidence missing ${variant}`);
}

const root = mkdtempSync(path.join(tmpdir(), 'radixdb-docs-sql-reference-'));
const cliLimited = new Set();
try {
  for (const [slug, sql] of examples) {
    const database = path.join(root, slug);
    const result = run(cli, ['-q', '-j', '-d', `file://${database}?sync_mode=full`, `--execute=${sql}`], {
      timeout: 30000,
    });
    assert.equal(result.status, 0, `${slug}: ${result.stderr}`);
    for (const line of result.stdout.trim().split('\n').filter(Boolean)) JSON.parse(line);
  }

  const savepointTest = run('cargo', [
    'test', '--locked', '--test', 'r3_l01_batch_b_statement_lifecycle_test',
    'r3_l01_batch_b_savepoint_identity_and_target_lifetime_are_stable', '--', '--exact',
  ], { cwd: worktree });
  assert.equal(savepointTest.status, 0, `${savepointTest.stdout}\n${savepointTest.stderr}`);
} finally {
  rmSync(root, { recursive: true, force: true });
}

const cleanAfter = run('git', ['status', '--short'], { cwd: worktree, timeout: 15000 });
assert.equal(cleanAfter.status, 0, cleanAfter.stderr);
assert.equal(cleanAfter.stdout, '');

console.log(JSON.stringify({
  identity: identity.stdout.trim(),
  command_pages: pages.size,
  matrix_contracts: expectedMatrixIds.size,
  direct_cli_examples: pages.size - cliLimited.size,
  verified_cli_boundaries: 2,
  engine_tests: 1,
  passed: true,
}, null, 2));
