import assert from 'node:assert/strict';
import { existsSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
const cli = process.env.RADIXDB_DOCS_CLI;
const worktree = process.env.RADIXDB_DOCS_WORKTREE;
const revision = process.env.RADIXDB_DOCS_REVISION
  ?? '40b1b3d13e050afa2666a0414b7215d5ac1452c0';
assert(cli && path.isAbsolute(cli), 'Set RADIXDB_DOCS_CLI to the pinned binary');
assert(worktree && path.isAbsolute(worktree), 'Set RADIXDB_DOCS_WORKTREE to the pinned worktree');

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    encoding: 'utf8', timeout: 180000, maxBuffer: 32 * 1024 * 1024, ...options,
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

function source(locale) {
  return readFileSync(path.join(root, 'src/content/docs', locale, 'appendices/compatibility.md'), 'utf8');
}

function rows(locale) {
  return source(locale).split('\n')
    .filter(line => /^\| [A-Z]+-\d+ \|/.test(line))
    .map(line => {
      const [id, construct, status, version, chapter, test] = line
        .split('|').slice(1, -1).map(value => value.trim());
      return { id, construct, status, version, chapter, test };
    });
}

function sqlBlocks(locale) {
  return [...source(locale).matchAll(/^```sql\n([\s\S]*?)^```/gm)].map(match => match[1]);
}

const en = rows('en');
const ru = rows('ru');
assert.equal(en.length, 83);
assert.deepEqual(en.map(row => row.id), ru.map(row => row.id));
assert.equal(new Set(en.map(row => row.id)).size, en.length);
assert.deepEqual(sqlBlocks('en'), sqlBlocks('ru'));
assert.equal(sqlBlocks('en').length, 2);

const statusTranslation = new Map([
  ['Supported', 'Поддерживается'],
  ['Limited', 'С ограничениями'],
  ['Rejected', 'Отклоняется'],
]);
for (let index = 0; index < en.length; index += 1) {
  const left = en[index];
  const right = ru[index];
  assert.equal(right.status, statusTranslation.get(left.status), left.id);
  assert.equal(left.version, '1.2', left.id);
  assert.equal(right.version, '1.2', left.id);
  assert.equal(left.test, right.test, left.id);

  const leftHref = left.chapter.match(/\]\(([^)]+)\)/)?.[1];
  const rightHref = right.chapter.match(/\]\(([^)]+)\)/)?.[1];
  assert(leftHref && rightHref, `missing chapter link: ${left.id}`);
  assert.equal(leftHref, rightHref, left.id);
  const chapterPath = leftHref.replace(/^\.\.\/\.\.\//, '').replace(/\/$/, '.md');
  assert(existsSync(path.join(root, 'src/content/docs/en', chapterPath)), `missing EN chapter: ${left.id}`);
  assert(existsSync(path.join(root, 'src/content/docs/ru', chapterPath)), `missing RU chapter: ${left.id}`);

  const testName = left.test.match(/`([^`]+\.mjs)`/)?.[1];
  assert(testName, `missing test evidence: ${left.id}`);
  assert(existsSync(path.join(root, 'scripts', testName)), `missing test file: ${left.id} ${testName}`);
}

for (const status of statusTranslation.keys()) {
  assert(en.some(row => row.status === status), `empty status: ${status}`);
}

const success = run(cli, ['-q', '-j', '-d', 'memory://docs_matrix_success',
  `--execute=${sqlBlocks('en')[0]}`], { timeout: 15000 });
assert.equal(success.status, 0, success.stderr);
const successRow = JSON.parse(success.stdout.trim());
assert.equal(successRow.rows[0][0].value, '7');

const rejected = run(cli, ['-q', '-j', '-d', 'memory://docs_matrix_rejected',
  `--execute=${sqlBlocks('en')[1]}`], { timeout: 15000 });
assert.notEqual(rejected.status, 0);
assert(rejected.stderr.includes('QUALIFY'), rejected.stderr);

const evidenceScripts = [
  'test-syntax.mjs',
  'test-values.mjs',
  'test-schema.mjs',
  'test-dml.mjs',
  'test-queries.mjs',
  'test-transactions.mjs',
  'test-navigation.mjs',
  'test-extensions.mjs',
];
assert.deepEqual([...new Set(en.map(row => row.test.match(/`([^`]+\.mjs)`/)[1]))].sort(),
  [...evidenceScripts].sort());
for (const script of evidenceScripts) {
  const result = run(process.execPath, [path.join(root, 'scripts', script)], { cwd: root });
  assert.equal(result.status, 0, `${script}\n${result.stdout}\n${result.stderr}`);
}

const cleanAfter = run('git', ['status', '--short'], { cwd: worktree, timeout: 15000 });
assert.equal(cleanAfter.status, 0, cleanAfter.stderr);
assert.equal(cleanAfter.stdout, '');

const counts = Object.fromEntries([...statusTranslation.keys()].map(status => [
  status.toLowerCase(), en.filter(row => row.status === status).length,
]));
console.log(JSON.stringify({
  identity: identity.stdout.trim(),
  contracts: en.length,
  ...counts,
  evidence_scripts: evidenceScripts.length,
  representative_successes: 1,
  representative_errors: 1,
  passed: true,
}, null, 2));
