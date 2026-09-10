import assert from 'node:assert/strict';
import { once } from 'node:events';
import {
  copyFileSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { spawn, spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
const worktree = process.env.RADIXDB_DOCS_WORKTREE;
const serverBin = process.env.RADIXDB_DOCS_SERVER;
const revision = process.env.RADIXDB_DOCS_REVISION;
for (const [name, value] of [
  ['RADIXDB_DOCS_WORKTREE', worktree],
  ['RADIXDB_DOCS_SERVER', serverBin],
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
    'programming', `${name}.md`), 'utf8');
}

function codeBlocks(source) {
  return [...source.matchAll(/^```[^\n]*\n([\s\S]*?)^```/gm)].map(match => match[1]);
}

function cargoTest(args) {
  const result = run('cargo', ['test', '--locked', ...args], { cwd: worktree });
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  const counts = [...`${result.stdout}\n${result.stderr}`.matchAll(
    /test result: ok\. (\d+) passed/g)].map(match => Number(match[1]));
  assert(counts.length > 0, result.stdout);
  const passed = counts.reduce((sum, count) => sum + count, 0);
  assert(passed > 0, `empty test selection: cargo test ${args.join(' ')}`);
  return passed;
}

async function freePort() {
  const probe = net.createServer();
  probe.listen(0, '127.0.0.1');
  await once(probe, 'listening');
  const address = probe.address();
  assert(address && typeof address !== 'string');
  const port = address.port;
  probe.close();
  await once(probe, 'close');
  return port;
}

async function waitForOutput(child, output, needle) {
  const deadline = Date.now() + 15000;
  while (!output.value.includes(needle)) {
    assert.equal(child.exitCode, null, `server exited before ${needle}: ${output.value}`);
    assert(Date.now() < deadline, `timeout waiting for ${needle}: ${output.value}`);
    await new Promise(resolve => setTimeout(resolve, 25));
  }
}

function tomlPath(value) {
  return value.replaceAll('\\', '\\\\').replaceAll('"', '\\"');
}

cleanWorktree();
const head = run('git', ['rev-parse', 'HEAD'], { cwd: worktree, timeout: 15000 });
assert.equal(head.status, 0, head.stderr);
assert.equal(head.stdout.trim(), revision);
const identity = run(serverBin, ['--version'], { timeout: 15000 });
assert.equal(identity.status, 0, identity.stderr);
assert(identity.stdout.includes(`git=${revision} `), identity.stdout);
assert(identity.stdout.includes('protocol=17 '), identity.stdout);
assert(identity.stdout.includes('profile=release '), identity.stdout);

const temp = mkdtempSync(path.join(os.tmpdir(), 'radixdb-docs-programming-'));
let child;
let marker;
try {
  copyFileSync(path.join(root, 'examples/programming/server_programming.rs'),
    path.join(temp, 'server_programming.rs'));
  writeFileSync(path.join(temp, 'Cargo.toml'), `[package]\nname = "radixdb-docs-programming"\nversion = "0.0.0"\nedition = "2021"\nrust-version = "1.97"\npublish = false\n\n[workspace]\n\n[dependencies]\nradixdb-client = { path = "${tomlPath(path.join(worktree, 'crates/radixdb-client'))}" }\n\n[[bin]]\nname = "server_programming"\npath = "server_programming.rs"\n`);
  const cargoEnv = { ...process.env, CARGO_TARGET_DIR: path.join(worktree, 'target') };
  let result = run('cargo', ['generate-lockfile', '--offline'], { cwd: temp, env: cargoEnv });
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  result = run('cargo', ['build', '--locked', '--offline'], {
    cwd: temp, env: cargoEnv,
  });
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);

  const port = await freePort();
  const config = path.join(temp, 'server.toml');
  writeFileSync(config, `[server]\nbind_ip = "127.0.0.1"\nport = ${port}\ndata_dir = "${tomlPath(path.join(temp, 'server-data'))}"\nmax_connections = 8\n`);
  child = spawn(serverBin, ['--config', config], { stdio: ['ignore', 'pipe', 'pipe'] });
  const output = { value: '' };
  child.stdout.on('data', chunk => { output.value += chunk; });
  child.stderr.on('data', chunk => { output.value += chunk; });
  await waitForOutput(child, output, 'radixdb-server listening on');

  const example = run(path.join(worktree, 'target/debug/server_programming'), [
    `127.0.0.1:${port}`, 'docs_programming',
  ], { timeout: 60000 });
  assert.equal(example.status, 0, `${example.stdout}\n${example.stderr}\n${output.value}`);
  marker = example.stdout.trim();
  assert(marker.includes('programming-ok function=42 flow=4 exception=300 cursor=304'));
  assert(marker.includes('rollback=verified trigger=2 trigger-error=atomic'));
  assert(marker.includes('job-scheduler=verified'));

  const exited = once(child, 'exit');
  assert(child.kill('SIGTERM'));
  const [code, signal] = await Promise.race([
    exited,
    new Promise((_, reject) => setTimeout(
      () => reject(new Error('server stop timeout')), 15000)),
  ]);
  assert.equal(signal, null);
  assert.equal(code, 0, output.value);
  assert(output.value.includes('radixdb-server stopped cleanly'), output.value);
  child = undefined;
} finally {
  if (child && child.exitCode === null && child.signalCode === null) {
    child.kill('SIGTERM');
    await Promise.race([once(child, 'exit'), new Promise(resolve => setTimeout(resolve, 15000))]);
  }
  rmSync(temp, { recursive: true, force: true });
}

const schedulerSource = readFileSync(path.join(worktree,
  'src/server/job_scheduler.rs'), 'utf8');
assert(schedulerSource.includes('pub(crate) fn run_loop('));
assert(schedulerSource.includes('radix_system_job_history'));
const jobSource = readFileSync(path.join(worktree,
  'crates/radixdb-executor/src/procedural/job.rs'), 'utf8');
assert(jobSource.includes('pub fn execute_job_attempt('));
assert(jobSource.includes('job attempts require a fresh executor transaction'));
assert(jobSource.includes('set_job_context('));
assert(jobSource.includes('DiagnosticKind::JobAttemptFailed'));
const contextSource = readFileSync(path.join(worktree,
  'crates/radixdb-executor/src/context.rs'), 'utf8');
for (const value of [
  'CURRENT_JOB_ID', 'CURRENT_JOB_ATTEMPT', 'CURRENT_JOB_SCHEDULED_AT',
  'CURRENT_IDEMPOTENCY_KEY',
]) assert(contextSource.includes(value), `missing ${value} job context support`);

const identifierHelper = run('rg', ['-n', 'SQL_IDENTIFIER',
  'crates/radixdb-procedural', 'crates/radixdb-executor',
  'crates/radixdb-sql', 'src/server'], { cwd: worktree, timeout: 15000 });
assert.equal(identifierHelper.status, 0, identifierHelper.stderr);

const statementSource = readFileSync(path.join(worktree,
  'crates/radixdb-sql/src/ast/statement.rs'), 'utf8');
for (const variant of ['DropRoutine', 'DropTrigger', 'DropJob', 'AlterJob']) {
  assert(statementSource.includes(`${variant}(`), `missing lifecycle variant ${variant}`);
}
const ownershipSource = readFileSync(path.join(worktree,
  'crates/radixdb-sql/src/ast/security.rs'), 'utf8');
assert(ownershipSource.includes('Function(RoutineSignatureSyntax)'));
assert(ownershipSource.includes('Procedure(RoutineSignatureSyntax)'));
assert(!ownershipSource.includes('Trigger('));
assert(!ownershipSource.includes('Job('));

const testCounts = [
  cargoTest(['-p', 'radixdb-sql', '--lib']),
  cargoTest(['-p', 'radixdb-procedural']),
  cargoTest(['-p', 'radixdb-executor', '--lib', 'procedural::']),
  cargoTest(['-p', 'radixdb-catalog', '--test', 'catalog_procedural_extension']),
];

let pairedCodeBlocks = 0;
for (const name of ['pl-sql', 'routines', 'triggers', 'jobs']) {
  const en = page('en', name);
  const ru = page('ru', name);
  const enBlocks = codeBlocks(en);
  assert.deepEqual(enBlocks, codeBlocks(ru), `${name} code differs between locales`);
  pairedCodeBlocks += enBlocks.length;
}
for (const [name, contracts] of Object.entries({
  'pl-sql': [
    'RadixDB PL', 'LANGUAGE RADIX', 'Rows::error()', 'EXECUTE',
    'PL_VERIFY_DYNAMIC_DDL_NOT_SUPPORTED', 'EXCEPTION', 'SQL_IDENTIFIER',
  ],
  routines: [
    'CREATE FUNCTION', 'CREATE PROCEDURE', 'CALL', 'RESOURCE POLICY',
    '10,000,000', '60 s', 'protocol 17', 'DROP FUNCTION',
  ],
  triggers: [
    'RETURNS TRIGGER', 'OLD', 'NEW', 'PRIORITY', 'PL_TRIGGER_CYCLE',
    'rollback', 'DROP TRIGGER',
  ],
  jobs: [
    'CREATE JOB', 'EVERY INTERVAL', 'AT TIMESTAMP',
    'radix_system_job_history', 'at-least-once', 'PL_JOB_ATTEMPT_FAILED', 'DROP JOB',
  ],
})) {
  const source = page('en', name);
  for (const contract of contracts) {
    assert(source.includes(contract), `${name} omits ${contract}`);
  }
}

cleanWorktree();
console.log(JSON.stringify({
  identity: identity.stdout.trim(),
  published_code_blocks: pairedCodeBlocks,
  executable_marker: marker,
  engine_tests: testCounts.reduce((sum, count) => sum + count, 0),
  test_groups: testCounts,
  stock_server_scheduler: true,
  identifier_helper: true,
  lifecycle_ddl: true,
  passed: true,
}, null, 2));
