import assert from 'node:assert/strict';
import { readFileSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

const cli = process.env.RADIXDB_DOCS_CLI;
const revision = process.env.RADIXDB_DOCS_REVISION
  ?? '40b1b3d13e050afa2666a0414b7215d5ac1452c0';
assert(cli && path.isAbsolute(cli), 'Set RADIXDB_DOCS_CLI to the pinned binary');
const identity = spawnSync(cli, ['--version'], { encoding: 'utf8', timeout: 15000 });
assert.equal(identity.status, 0, identity.stderr);
assert(identity.stdout.includes(`git=${revision} `), identity.stdout);
const blocks = locale => [...readFileSync(new URL(`../src/content/docs/${locale}/sql/dml.md`, import.meta.url), 'utf8')
  .matchAll(/^```sql\n([\s\S]*?)^```/gm)].map(match => match[1]);
const sqlBlocks = blocks('en');
assert.deepEqual(sqlBlocks, blocks('ru'));
const finalRows = [['1', 'renamed', true, '2'], ['3', 'archive', false, '1']];
const expectedQueries = [[], [[['3', 'archive', false]]], [[['1', true, '2']]], [], [],
  [[['1', 'renamed']]], [[['2', 'publish']], finalRows], [[['renamed']]]];
assert.equal(sqlBlocks.length, expectedQueries.length);
const directory = mkdtempSync(path.join(tmpdir(), 'radixdb-docs-dml-'));
function run(sql, failure = false) {
  const result = spawnSync(cli, ['-q', '-j', '-d', `file://${directory}/database?sync_mode=full`, `--execute=${sql}`], {
    encoding: 'utf8', timeout: 30000,
  });
  assert(!result.error, String(result.error));
  assert.equal(result.signal, null);
  if (failure) {
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /primary key constraint/i);
    return;
  }
  assert.equal(result.status, 0, result.stderr);
  return result.stdout.trim().split('\n').map(line => JSON.parse(line));
}
function rows(output) {
  return output.filter(item => Array.isArray(item.rows)).map(item => {
    assert.equal(item.truncated, false);
    assert.equal(item.count, item.rows.length);
    return item.rows.map(row => row.map(cell => cell.value));
  });
}
try {
  for (const [index, sql] of sqlBlocks.entries()) {
    const output = run(sql);
    assert.deepEqual(rows(output), expectedQueries[index]);
    if (index === 0) assert.deepEqual(output, [{ rows_affected: 0 }, { rows_affected: 2 }]);
    if (index === 3 || index === 4) assert.deepEqual(output, [{ rows_affected: 0 }]);
  }
  run("INSERT INTO tasks (id, title) VALUES (4, 'must roll back'), (1, 'duplicate');", true);
  assert.deepEqual(rows(run('SELECT id, title, done, revision FROM tasks ORDER BY id;')), [finalRows]);
  console.log(JSON.stringify({ identity: identity.stdout.trim(), blocks: sqlBlocks.length, atomic_conflict: true, passed: true }, null, 2));
} finally {
  rmSync(directory, { recursive: true, force: true });
}
