import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import path from 'node:path';

const cli = process.env.RADIXDB_DOCS_CLI;
const revision = process.env.RADIXDB_DOCS_REVISION
  ?? '40b1b3d13e050afa2666a0414b7215d5ac1452c0';
assert(cli && path.isAbsolute(cli), 'Set RADIXDB_DOCS_CLI to the pinned binary');
const identity = spawnSync(cli, ['--version'], { encoding: 'utf8', timeout: 15000 });
assert.equal(identity.status, 0, identity.stderr);
assert(identity.stdout.includes(`git=${revision} `), identity.stdout);
function run(sql) {
  const result = spawnSync(cli, ['-q', '-j', '-d', 'memory://', `--execute=${sql}`], {
    encoding: 'utf8', timeout: 15000,
  });
  assert(!result.error, String(result.error));
  assert.equal(result.signal, null);
  return result;
}
function blocks(locale, chapter) {
  const source = readFileSync(new URL(`../src/content/docs/${locale}/sql/${chapter}.md`, import.meta.url), 'utf8');
  return [...source.matchAll(/^```sql\n([\s\S]*?)^```/gm)].map(match => match[1]);
}
const expected = {
  types: [
    { commands: [0, 1], values: ['100000', 'ready', true], types: ['INTEGER', 'TEXT', 'BOOLEAN'] },
    { commands: [0, 1], values: ['12.34'], types: ['TEXT'] },
    { commands: [], values: [20704, '01940000-0020-7000-8000-000000000001', '00ff7f'], types: ['DATE', 'UUID', 'BYTES'] },
  ],
  expressions: [
    { values: ['14', '20', '3', '1'], types: ['INTEGER', 'INTEGER', 'INTEGER', 'INTEGER'] },
    { values: ['3.5', null], types: ['FLOAT', 'NULL'] },
    { values: [null, true, null, false, true, null], types: ['NULL', 'BOOLEAN', 'NULL', 'BOOLEAN', 'BOOLEAN', 'NULL'] },
    { values: [null, true], types: ['NULL', 'BOOLEAN'] },
    { values: ['7', null, '10'], types: ['INTEGER', 'NULL', 'INTEGER'] },
    { values: ['abcd', true, 'hello'], types: ['TEXT', 'BOOLEAN', 'TEXT'] },
    { values: ['42', '2', null], types: ['TEXT', 'INTEGER', 'NULL'] },
  ],
};
let examples = 0;
for (const chapter of Object.keys(expected)) {
  const sqlBlocks = blocks('en', chapter);
  assert.deepEqual(sqlBlocks, blocks('ru', chapter));
  assert.equal(sqlBlocks.length, expected[chapter].length);
  for (const [index, sql] of sqlBlocks.entries()) {
    const result = run(sql);
    assert.equal(result.status, 0, result.stderr);
    const objects = result.stdout.trim().split('\n').map(line => JSON.parse(line));
    const expectation = expected[chapter][index];
    assert.deepEqual(objects.slice(0, -1), (expectation.commands || []).map(rows_affected => ({ rows_affected })));
    const output = objects.at(-1);
    assert.equal(output.count, 1);
    assert.equal(output.truncated, false);
    assert.equal(output.rows.length, 1);
    assert.deepEqual(output.rows[0].map(cell => cell.type), expectation.types);
    const value = cell => cell.type === 'DATE' ? cell.days_since_unix_epoch
      : cell.type === 'BYTES' ? cell.raw_hex : cell.value;
    assert.deepEqual(output.rows[0].map(value), expectation.values);
    examples++;
  }
}
const invalid = [
  ['CREATE TABLE bad (v VARCHAR(2))', 'type modifiers are not supported'],
  ['CREATE TABLE bad (v CHAR(10))', 'type modifiers are not supported'],
  ['CREATE TABLE bad (v DECIMAL(39,2))', 'precision'],
  ['CREATE TABLE bad (v DECIMAL(4,5))', 'scale'],
  ['CREATE TABLE bad (v VECTOR(0))', 'dimension'],
  ["SELECT FROM_HEX('0xz')", 'hex'],
];
for (const [sql, diagnostic] of invalid) {
  const result = run(sql);
  assert.notEqual(result.status, 0, sql);
  assert(result.stderr.toLowerCase().includes(diagnostic), result.stderr);
}
console.log(JSON.stringify({ identity: identity.stdout.trim(), examples, rejected_inputs: invalid.length, passed: true }, null, 2));
