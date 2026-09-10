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
    'clients', `${name}.md`), 'utf8');
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
  return Math.max(...counts);
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

function rustPath(value) {
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

let pairedCodeBlocks = 0;
for (const name of ['embedded-rust', 'rust-client', 'orm']) {
  const en = page('en', name);
  const ru = page('ru', name);
  const enBlocks = codeBlocks(en);
  assert.deepEqual(enBlocks, codeBlocks(ru), `${name} code differs between locales`);
  pairedCodeBlocks += enBlocks.length;
}

for (const [name, contracts] of Object.entries({
  'embedded-rust': [
    'Database::open', 'query_one', 'get_by_name', 'transaction.rollback()',
    'Rows::error()', 'Database::close()',
  ],
  'rust-client': [
    'connect_with_timeouts', 'execute_prepared', 'CommandsOutOfSync',
    'is_retryable()', 'outcome is unknown', 'is_reusable()',
  ],
  orm: [
    'DynamicRecord', 'Reference<T>', 'SchemaChanged',
    'DESCRIBE DATABASE FORMAT JSON', 'identity map', 'connection.begin()',
  ],
})) {
  const source = page('en', name);
  for (const contract of contracts) {
    assert(source.includes(contract), `${name} omits ${contract}`);
  }
}

const publicOrmExample = readFileSync(path.join(worktree,
  'examples/public/rust-orm/orm_quickstart.rs'), 'utf8');
assert(publicOrmExample.includes('connection.begin()?'));
assert(publicOrmExample.includes('connection.rollback()?'));
assert(publicOrmExample.includes('--transaction-smoke'));
const clientSource = readFileSync(path.join(worktree,
  'crates/radixdb-client/src/lib.rs'), 'utf8');
assert(clientSource.includes('Transport failures remain outcome-unknown'));
assert(clientSource.includes('ClientError::CommandsOutOfSync'));
const asyncSource = readFileSync(path.join(worktree,
  'crates/radixdb-client/src/async_client.rs'), 'utf8');
assert(asyncSource.includes('future is dropped after frame I/O may'));
assert(asyncSource.includes('self.state.poison()'));

const temp = mkdtempSync(path.join(os.tmpdir(), 'radixdb-docs-clients-'));
let child;
try {
  for (const name of ['embedded', 'tcp', 'orm']) {
    copyFileSync(path.join(root, 'examples/clients', `${name}.rs`),
      path.join(temp, `${name}.rs`));
  }
  writeFileSync(path.join(temp, 'Cargo.toml'), `[package]\nname = "radixdb-docs-clients"\nversion = "0.0.0"\nedition = "2021"\nrust-version = "1.97"\npublish = false\n\n[workspace]\n\n[dependencies]\nradixdb = { path = "${rustPath(worktree)}" }\nradixdb-client = { path = "${rustPath(path.join(worktree, 'crates/radixdb-client'))}" }\nradixdb-orm = { path = "${rustPath(path.join(worktree, 'crates/radixdb-orm'))}" }\n\n[[bin]]\nname = "embedded"\npath = "embedded.rs"\n\n[[bin]]\nname = "tcp"\npath = "tcp.rs"\n\n[[bin]]\nname = "orm"\npath = "orm.rs"\n`);
  const cargoEnv = { ...process.env, CARGO_TARGET_DIR: path.join(worktree, 'target') };
  let result = run('cargo', ['generate-lockfile', '--offline'], { cwd: temp, env: cargoEnv });
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  result = run('cargo', ['build', '--locked', '--offline', '--bins'], {
    cwd: temp, env: cargoEnv,
  });
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);

  const bin = name => path.join(worktree, 'target', 'debug', name);
  const embedded = run(bin('embedded'), [path.join(temp, 'embedded-database')], {
    timeout: 60000,
  });
  assert.equal(embedded.status, 0, `${embedded.stdout}\n${embedded.stderr}`);
  assert(embedded.stdout.includes(
    'embedded-ok rows=2 duplicate=rejected rollback=verified'));

  const port = await freePort();
  const config = path.join(temp, 'server.toml');
  writeFileSync(config, `[server]\nbind_ip = "127.0.0.1"\nport = ${port}\ndata_dir = "${rustPath(path.join(temp, 'server-data'))}"\nmax_connections = 8\n`);
  child = spawn(serverBin, ['--config', config], { stdio: ['ignore', 'pipe', 'pipe'] });
  const output = { value: '' };
  child.stdout.on('data', chunk => { output.value += chunk; });
  child.stderr.on('data', chunk => { output.value += chunk; });
  await waitForOutput(child, output, 'radixdb-server listening on');

  const address = `127.0.0.1:${port}`;
  const tcp = run(bin('tcp'), [address, 'docs_clients'], { timeout: 60000 });
  assert.equal(tcp.status, 0, `${tcp.stdout}\n${tcp.stderr}\n${output.value}`);
  assert(tcp.stdout.includes(
    'tcp-ok rows=2 duplicate=rejected cursor=closed rollback=verified'));
  const orm = run(bin('orm'), [address, 'docs_clients'], { timeout: 60000 });
  assert.equal(orm.status, 0, `${orm.stdout}\n${orm.stderr}\n${output.value}`);
  assert(orm.stdout.includes(
    'orm-ok reference=validated crud=verified shared-rollback=verified'));

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

  const clientTests = cargoTest(['-p', 'radixdb-client', '--lib']);
  const transactionTests = cargoTest(['--test', 'tcp_transaction_error_atomicity_test']);
  const ormTests = cargoTest(['--test', 'orm_contract_test']);
  const codegenTests = cargoTest(['--test', 'orm_codegen_runtime_test']);

  cleanWorktree();
  console.log(JSON.stringify({
    identity: identity.stdout.trim(),
    published_code_blocks: pairedCodeBlocks,
    examples: ['embedded', 'tcp', 'orm'],
    embedded: embedded.stdout.trim(),
    tcp: tcp.stdout.trim(),
    orm: orm.stdout.trim(),
    engine_tests: clientTests + transactionTests + ormTests + codegenTests,
    public_orm_transaction_smoke: 'verified',
    passed: true,
  }, null, 2));
} finally {
  if (child && child.exitCode === null && child.signalCode === null) {
    child.kill('SIGTERM');
    await Promise.race([
      once(child, 'exit'),
      new Promise(resolve => setTimeout(resolve, 15000)),
    ]);
  }
  rmSync(temp, { recursive: true, force: true });
}
