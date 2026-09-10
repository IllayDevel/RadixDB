import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { existsSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { parse } from 'smol-toml';

const root = fileURLToPath(new URL('../', import.meta.url));
const repo = path.resolve(root, '..');
const registry = parse(readFileSync(path.join(root, '_meta/example-gates.toml'), 'utf8'));
const chapters = parse(readFileSync(path.join(root, '_meta/chapters.toml'), 'utf8'));
const packageJson = JSON.parse(readFileSync(path.join(root, 'package.json'), 'utf8'));
const worktree = process.env.RADIXDB_DOCS_WORKTREE;
const revision = process.env.RADIXDB_DOCS_REVISION;

for (const [name, value] of [
  ['RADIXDB_DOCS_WORKTREE', worktree],
  ['RADIXDB_DOCS_REVISION', revision],
  ['RADIXDB_DOCS_CLI', process.env.RADIXDB_DOCS_CLI],
  ['RADIXDB_DOCS_SERVER', process.env.RADIXDB_DOCS_SERVER],
  ['RADIXDB_DOCS_SMOKE', process.env.RADIXDB_DOCS_SMOKE],
]) {
  if (name === 'RADIXDB_DOCS_REVISION') {
    assert(/^[a-f0-9]{40}$/.test(value || ''), `Set ${name} to the pinned revision`);
  } else {
    assert(value && path.isAbsolute(value), `Set ${name} to the pinned absolute path`);
    if (name === 'RADIXDB_DOCS_WORKTREE') {
      assert.equal(path.resolve(value), value, `${name} must be normalized`);
    } else {
      const relative = path.relative(worktree, value);
      assert(relative && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative),
        `${name} must point inside the pinned worktree`);
    }
  }
}

assert.equal(revision, registry.baseline, 'Example registry and requested baseline differ');

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: repo,
    encoding: 'utf8',
    timeout: 900000,
    maxBuffer: 128 * 1024 * 1024,
    ...options,
  });
  assert(!result.error, String(result.error));
  assert.equal(result.signal, null, `${command} was terminated by ${result.signal}`);
  return result;
}

function assertCleanBaseline() {
  const head = run('git', ['rev-parse', 'HEAD'], { cwd: worktree, timeout: 15000 });
  assert.equal(head.status, 0, head.stderr);
  assert.equal(head.stdout.trim(), revision);
  const status = run('git', ['status', '--short'], { cwd: worktree, timeout: 15000 });
  assert.equal(status.status, 0, status.stderr);
  assert.equal(status.stdout, '', 'Pinned verification worktree is dirty');
}

function lastJson(output) {
  const starts = [...output.matchAll(/(?:^|\n)(?=\{)/g)].map(match => match.index + (match[0] === '\n' ? 1 : 0));
  for (const start of starts.reverse()) {
    try { return JSON.parse(output.slice(start).trim()); } catch {}
  }
  assert.fail('Gate did not emit a final JSON object');
}

function compact(payload) {
  return Object.fromEntries(Object.entries(payload).flatMap(([key, value]) => {
    if (['identity', 'binary_sha256', 'passed'].includes(key)) return [];
    if (Array.isArray(value)) return [[key, value.length]];
    if (['string', 'number', 'boolean'].includes(typeof value)) return [[key, value]];
    return [];
  }));
}

const ids = new Set();
const chapterTestScripts = new Set(chapters.chapters.flatMap(chapter => chapter.tests).flatMap(command => {
  const direct = command.match(/^node (scripts\/test-[a-z0-9-]+\.mjs)$/);
  if (direct) return [direct[1]];
  const npm = command.match(/^npm run ([a-z0-9:-]+)$/);
  const target = npm && packageJson.scripts[npm[1]];
  const indirect = target?.match(/^node (scripts\/test-[a-z0-9-]+\.mjs)$/);
  return indirect ? [indirect[1]] : [];
}));
for (const gate of registry.gates) {
  assert(!ids.has(gate.id), `Duplicate example gate: ${gate.id}`);
  ids.add(gate.id);
  assert(/^[a-z0-9-]+$/.test(gate.id), `Invalid example gate ID: ${gate.id}`);
  assert(Array.isArray(gate.kinds) && gate.kinds.length > 0);
  assert(gate.kinds.every(kind => ['sql', 'rust', 'cli'].includes(kind)), `Invalid kinds: ${gate.id}`);
  assert(/^scripts\/test-[a-z0-9-]+\.mjs$/.test(gate.script), `Unsafe script path: ${gate.script}`);
  assert(existsSync(path.join(root, gate.script)), `Missing gate script: ${gate.script}`);
  assert(chapterTestScripts.has(gate.script), `Gate is not assigned to a chapter: ${gate.script}`);
}

assert.deepEqual(registry.project_examples, [],
  'All tracked public examples must be covered by executable gates');

assertCleanBaseline();
const content = run(process.execPath, [path.join(root, 'scripts/check-content.mjs')]);
assert.equal(content.status, 0, `${content.stdout}\n${content.stderr}`);

const results = [];
for (const gate of registry.gates) {
  process.stderr.write(`[examples] ${gate.id}\n`);
  const result = run(process.execPath, [path.join(root, gate.script)]);
  assert.equal(result.status, 0, `${gate.id}\n${result.stdout}\n${result.stderr}`);
  const payload = lastJson(result.stdout);
  if ('passed' in payload) assert.equal(payload.passed, true, `${gate.id} did not pass`);
  if (Array.isArray(payload.results)) {
    assert(payload.results.length > 0 && payload.results.every(item => item.passed === true),
      `${gate.id} contains a failed result`);
  }
  results.push({
    id: gate.id,
    kinds: gate.kinds,
    script: gate.script,
    output_sha256: createHash('sha256').update(result.stdout).digest('hex'),
    details: compact(payload),
    passed: true,
  });
  assertCleanBaseline();
}

console.log(JSON.stringify({
  revision,
  content_contract: true,
  verified_gates: results.length,
  gates: results,
  passed: true,
}, null, 2));
