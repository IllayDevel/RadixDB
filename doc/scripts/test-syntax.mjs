import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import path from 'node:path';

const revision = process.env.RADIXDB_DOCS_REVISION
  ?? '40b1b3d13e050afa2666a0414b7215d5ac1452c0';
const cli = process.env.RADIXDB_DOCS_CLI;
assert(cli && path.isAbsolute(cli), 'Set RADIXDB_DOCS_CLI to the pinned CLI binary');
const identity = spawnSync(cli, ['--version'], { encoding: 'utf8', timeout: 15000 });
assert.equal(identity.status, 0, identity.stderr);
assert(identity.stdout.includes(`git=${revision} `), identity.stdout);
const blocks = locale => [...readFileSync(new URL(`../src/content/docs/${locale}/sql/syntax.md`, import.meta.url), 'utf8')
  .matchAll(/^```sql\n([\s\S]*?)^```/gm)].map(match => match[1]);
const en = blocks('en');
assert.deepEqual(en, blocks('ru'));
const expected = [
  [[['1']], [['1']]],
  [[['7', '8']]],
  [[['O\'Brien', '', true, null]]],
  [[['42', '-7', '1.25', '2000']]],
  [[['3']]],
];
assert.equal(en.length, expected.length);
for (const [index, sql] of en.entries()) {
  const result = spawnSync(cli, ['-q', '-j', '-d', 'memory://', `--execute=${sql}`], { encoding: 'utf8', timeout: 15000 });
  assert.equal(result.status, 0, result.stderr);
  const output = result.stdout.trim().split('\n').map(line => JSON.parse(line));
  assert(output.every(item => !item.truncated));
  assert.deepEqual(output.map(item => item.rows.map(row => row.map(cell => cell.value))), expected[index]);
}
for (const sql of ["SELECT 'unterminated", 'SELECT /* unfinished', 'SELECT 1e', 'SELECT $0', 'SELECT ?, $1']) {
  const result = spawnSync(cli, ['-q', '-d', 'memory://', `--execute=${sql}`], { encoding: 'utf8', timeout: 15000 });
  assert(!result.error, String(result.error));
  assert.equal(result.signal, null);
  assert.notEqual(result.status, 0, `Expected rejection: ${sql}`);
  assert(result.stderr.trim(), `Missing diagnostic: ${sql}`);
}
console.log(JSON.stringify({ identity: identity.stdout.trim(), example_blocks: en.length, rejected_inputs: 5, passed: true }, null, 2));
